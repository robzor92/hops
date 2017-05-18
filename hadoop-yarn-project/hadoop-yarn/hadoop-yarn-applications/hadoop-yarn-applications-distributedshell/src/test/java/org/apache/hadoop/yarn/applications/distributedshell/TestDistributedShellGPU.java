/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.applications.distributedshell;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.JarFinder;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomain;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculatorGPU;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.InetAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestDistributedShellGPU {

  private static final Log LOG =
      LogFactory.getLog(TestDistributedShellGPU.class);

  protected MiniYARNCluster yarnCluster = null;
  protected YarnConfiguration conf = null;
  private static final int NUM_NMS = 2;

  protected final static String APPMASTER_JAR =
      JarFinder.getJar(ApplicationMaster.class);

  @Before
  public void setup() throws Exception {
    setupInternal(NUM_NMS);
  }

  protected void setupInternal(int numNodeManager) throws Exception {

    LOG.info("Starting up YARN cluster");

    CapacitySchedulerConfiguration csconf =
            new CapacitySchedulerConfiguration();
    csconf.setResourceComparator(DominantResourceCalculatorGPU.class);

    conf = new YarnConfiguration(csconf);
    conf.setInt(YarnConfiguration.NM_GPUS, 2);
    conf.setInt(YarnConfiguration.NM_VCORES, 32);
    conf.setInt(YarnConfiguration.YARN_MINICLUSTER_NM_PMEM_MB, 102400);

    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_GPUS, 0);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_GPUS, 4);

    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES, 1);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES, 8);

    conf.set("yarn.log.dir", "target");
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    conf.set(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class.getName());
    conf.setBoolean(YarnConfiguration.NODE_LABELS_ENABLED, true);

    if (yarnCluster == null) {
      yarnCluster =
          new MiniYARNCluster(TestDistributedShellGPU.class.getSimpleName(), 1,
              numNodeManager, 1, 1);
      yarnCluster.init(conf);

      yarnCluster.start();

      conf.set(
          YarnConfiguration.TIMELINE_SERVICE_WEBAPP_ADDRESS,
          MiniYARNCluster.getHostname() + ":"
              + yarnCluster.getApplicationHistoryServer().getPort());

      waitForNMsToRegister();

      URL url = Thread.currentThread().getContextClassLoader().getResource("yarn-site.xml");
      if (url == null) {
        throw new RuntimeException("Could not find 'yarn-site.xml' dummy file in classpath");
      }
      Configuration yarnClusterConfig = yarnCluster.getConfig();
      yarnClusterConfig.set("yarn.application.classpath", new File(url.getPath()).getParent());
      //write the document to a buffer (not directly to the file, as that
      //can cause the file being written to get read -which will then fail.
      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      yarnClusterConfig.writeXml(bytesOut);
      bytesOut.close();
      //write the bytes to the file in the classpath
      OutputStream os = new FileOutputStream(new File(url.getPath()));
      os.write(bytesOut.toByteArray());
      os.close();
    }
    FileContext fsContext = FileContext.getLocalFSFileContext();
    fsContext
        .delete(
            new Path(conf
                .get("yarn.timeline-service.leveldb-timeline-store.path")),
            true);
    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      LOG.info("setup thread sleep interrupted. message=" + e.getMessage());
    }
  }

  @After
  public void tearDown() throws IOException {
    if (yarnCluster != null) {
      try {
        yarnCluster.stop();
      } finally {
        yarnCluster = null;
      }
    }
    FileContext fsContext = FileContext.getLocalFSFileContext();
    fsContext
        .delete(
            new Path(conf
                .get("yarn.timeline-service.leveldb-timeline-store.path")),
            true);
  }

  @Test
  public void bootUp() {

  }

  /*
   * NetUtils.getHostname() returns a string in the form "hostname/ip".
   * Sometimes the hostname we get is the FQDN and sometimes the short name. In
   * addition, on machines with multiple network interfaces, it runs any one of
   * the ips. The function below compares the returns values for
   * NetUtils.getHostname() accounting for the conditions mentioned.
   */
  private boolean checkHostname(String appHostname) throws Exception {

    String hostname = NetUtils.getHostname();
    if (hostname.equals(appHostname)) {
      return true;
    }

    Assert.assertTrue("Unknown format for hostname " + appHostname,
      appHostname.contains("/"));
    Assert.assertTrue("Unknown format for hostname " + hostname,
      hostname.contains("/"));

    String[] appHostnameParts = appHostname.split("/");
    String[] hostnameParts = hostname.split("/");

    return (compareFQDNs(appHostnameParts[0], hostnameParts[0]) && checkIPs(
      hostnameParts[0], hostnameParts[1], appHostnameParts[1]));
  }

  private boolean compareFQDNs(String appHostname, String hostname)
      throws Exception {
    if (appHostname.equals(hostname)) {
      return true;
    }
    String appFQDN = InetAddress.getByName(appHostname).getCanonicalHostName();
    String localFQDN = InetAddress.getByName(hostname).getCanonicalHostName();
    return appFQDN.equals(localFQDN);
  }

  private boolean checkIPs(String hostname, String localIP, String appIP)
      throws Exception {

    if (localIP.equals(appIP)) {
      return true;
    }
    boolean appIPCheck = false;
    boolean localIPCheck = false;
    InetAddress[] addresses = InetAddress.getAllByName(hostname);
    for (InetAddress ia : addresses) {
      if (ia.getHostAddress().equals(appIP)) {
        appIPCheck = true;
        continue;
      }
      if (ia.getHostAddress().equals(localIP)) {
        localIPCheck = true;
      }
    }
    return (appIPCheck && localIPCheck);

  }

  @Test(timeout=90000)
  public void testDSShellWithShellScript() throws Exception {
    final File basedir =
        new File("target", TestDistributedShellGPU.class.getName());
    final File tmpDir = new File(basedir, "tmpDir");
    tmpDir.mkdirs();
    final File customShellScript = new File(tmpDir, "custom_script.sh");
    if (customShellScript.exists()) {
      customShellScript.delete();
    }
    if (!customShellScript.createNewFile()) {
      Assert.fail("Can not create custom shell script file.");
    }
    PrintWriter fileWriter = new PrintWriter(customShellScript);
    // set the output to DEBUG level
    fileWriter.write("sleep 20\necho Done");
    fileWriter.close();
    System.out.println(customShellScript.getAbsolutePath());
    String[] args = {
        "--jar",
        APPMASTER_JAR,
        "--num_containers",
        "2",
        "--shell_script",
        customShellScript.getAbsolutePath(),
        "--master_memory",
        "1024",
        "--master_vcores",
        "1",
        "--container_memory",
        "4092",
        "--container_vcores",
        "4",
        "--container_gpus",
        "1"
    };

    LOG.info("Initializing DS Client");
    final Client client =
        new Client(new Configuration(yarnCluster.getConfig()));
    boolean initSuccess = client.init(args);
    Assert.assertTrue(initSuccess);
    LOG.info("Running DS Client");
    boolean result = client.run();
    LOG.info("Client run completed. Result=" + result);
    List<String> expectedContent = new ArrayList<String>();
    expectedContent.add("Done");
    verifyContainerLog(1, expectedContent, false, "");
  }

  @Test(timeout=90000)
  public void testDSShellWithShellCommand() throws Exception {
    String[] args = {
            "--jar",
            APPMASTER_JAR,
            "--num_containers",
            "5",
            "--shell_command",
            "printenv",
            "--master_memory",
            "1024",
            "--master_vcores",
            "2",
            "--container_memory",
            "2048",
            "--container_vcores",
            "8",
            "--container_gpus",
            "1"
    };

    LOG.info("Initializing DS Client");
    final Client client =
            new Client(new Configuration(yarnCluster.getConfig()));
    boolean initSuccess = client.init(args);
    Assert.assertTrue(initSuccess);
    LOG.info("Running DS Client");
    boolean result = client.run();
    LOG.info("Client run completed. Result=" + result);
    List<String> expectedContent = new ArrayList<String>();
    expectedContent.add("HADOOP YARN MAPREDUCE HDFS");
    verifyContainerLog(2, expectedContent, false, "");
  }

  protected void waitForNMsToRegister() throws Exception {
    int sec = 60;
    while (sec >= 0) {
      if (yarnCluster.getResourceManager().getRMContext().getRMNodes().size()
          >= NUM_NMS) {
        break;
      }
      Thread.sleep(1000);
      sec--;
    }
  }

  private int verifyContainerLog(int containerNum,
      List<String> expectedContent, boolean count, String expectedWord) {
    File logFolder =
        new File(yarnCluster.getNodeManager(0).getConfig()
            .get(YarnConfiguration.NM_LOG_DIRS,
                YarnConfiguration.DEFAULT_NM_LOG_DIRS));

    File[] listOfFiles = logFolder.listFiles();
    int currentContainerLogFileIndex = -1;
    for (int i = listOfFiles.length - 1; i >= 0; i--) {
      if (listOfFiles[i].listFiles().length == containerNum + 1) {
        currentContainerLogFileIndex = i;
        break;
      }
    }
    Assert.assertTrue(currentContainerLogFileIndex != -1);
    File[] containerFiles =
        listOfFiles[currentContainerLogFileIndex].listFiles();

    int numOfWords = 0;
    for (int i = 0; i < containerFiles.length; i++) {
      for (File output : containerFiles[i].listFiles()) {
        if (output.getName().trim().contains("stdout")) {
          BufferedReader br = null;
          List<String> stdOutContent = new ArrayList<String>();
          try {

            String sCurrentLine;
            br = new BufferedReader(new FileReader(output));
            int numOfline = 0;
            while ((sCurrentLine = br.readLine()) != null) {
              if (count) {
                if (sCurrentLine.contains(expectedWord)) {
                  numOfWords++;
                }
              } else if (output.getName().trim().equals("stdout")){
                if (! Shell.WINDOWS) {
                  Assert.assertEquals("The current is" + sCurrentLine,
                      expectedContent.get(numOfline), sCurrentLine.trim());
                  numOfline++;
                } else {
                  stdOutContent.add(sCurrentLine.trim());
                }
              }
            }
            /* By executing bat script using cmd /c,
             * it will output all contents from bat script first
             * It is hard for us to do check line by line
             * Simply check whether output from bat file contains
             * all the expected messages
             */
            if (Shell.WINDOWS && !count
                && output.getName().trim().equals("stdout")) {
              Assert.assertTrue(stdOutContent.containsAll(expectedContent));
            }
          } catch (IOException e) {
            e.printStackTrace();
          } finally {
            try {
              if (br != null)
                br.close();
            } catch (IOException ex) {
              ex.printStackTrace();
            }
          }
        }
      }
    }
    return numOfWords;
  }
}

