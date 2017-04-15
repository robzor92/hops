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
package org.apache.hadoop.yarn.server.nodemanager.util;

import io.hops.GPUManagementLibrary;
import io.hops.devices.Device;
import io.hops.devices.GPUAllocator;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.nodemanager.LinuxContainerExecutor;
import org.apache.hadoop.yarn.util.ResourceCalculatorPlugin;
import org.junit.Assert;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Clock;
import org.junit.Test;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import java.io.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

public class TestCgroupsLCEResourcesHandler {
  static File cgroupDir = null;

  static class MockClock implements Clock {
    long time;

    @Override
    public long getTime() {
      return time;
    }
  }

  @Before
  public void setUp() throws Exception {
    cgroupDir =
            new File(System.getProperty("test.build.data",
                    System.getProperty("java.io.tmpdir", "target")), this.getClass()
                    .getName());
    FileUtils.deleteQuietly(cgroupDir);
  }

  @After
  public void tearDown() throws Exception {
    FileUtils.deleteQuietly(cgroupDir);
  }

  @Test
  public void testcheckAndDeleteCgroup() throws Exception {
    CgroupsLCEResourcesHandler handler = new CgroupsLCEResourcesHandler();
    handler.setConf(new YarnConfiguration());
    handler.initConfig();

    FileUtils.deleteQuietly(cgroupDir);
    // Test 0
    // tasks file not present, should return false
    Assert.assertFalse(handler.checkAndDeleteCgroup(cgroupDir));

    File tfile = new File(cgroupDir.getAbsolutePath(), "tasks");
    FileOutputStream fos = FileUtils.openOutputStream(tfile);
    File fspy = Mockito.spy(cgroupDir);

    // Test 1, tasks file is empty
    // tasks file has no data, should return true
    Mockito.stub(fspy.delete()).toReturn(true);
    Assert.assertTrue(handler.checkAndDeleteCgroup(fspy));

    // Test 2, tasks file has data
    fos.write("1234".getBytes());
    fos.close();
    // tasks has data, would not be able to delete, should return false
    Assert.assertFalse(handler.checkAndDeleteCgroup(fspy));
    FileUtils.deleteQuietly(cgroupDir);

  }

  // Verify DeleteCgroup times out if "tasks" file contains data
  @Test
  public void testDeleteCgroup() throws Exception {
    final MockClock clock = new MockClock();
    clock.time = System.currentTimeMillis();
    CgroupsLCEResourcesHandler handler = new CgroupsLCEResourcesHandler();
    handler.setConf(new YarnConfiguration());
    handler.initConfig();
    handler.clock = clock;

    FileUtils.deleteQuietly(cgroupDir);

    // Create a non-empty tasks file
    File tfile = new File(cgroupDir.getAbsolutePath(), "tasks");
    FileOutputStream fos = FileUtils.openOutputStream(tfile);
    fos.write("1234".getBytes());
    fos.close();

    final CountDownLatch latch = new CountDownLatch(1);
    new Thread() {
      @Override
      public void run() {
        latch.countDown();
        try {
          Thread.sleep(200);
        } catch (InterruptedException ex) {
          //NOP
        }
        clock.time += YarnConfiguration.
                DEFAULT_NM_LINUX_CONTAINER_CGROUPS_DELETE_TIMEOUT;
      }
    }.start();
    latch.await();
    Assert.assertFalse(handler.deleteCgroup(cgroupDir.getAbsolutePath()));
    FileUtils.deleteQuietly(cgroupDir);
  }

  static class MockLinuxContainerExecutor extends LinuxContainerExecutor {
    @Override
    public void mountCgroups(List<String> x, String y) {
    }
  }

  static class CustomCgroupsLCEResourceHandler extends
          CgroupsLCEResourcesHandler {

    String mtabFile;
    GPUAllocator gpuAllocator;
    int[] limits = new int[2];
    boolean generateLimitsMode = false;

    @Override
    int[] getOverallLimits(float x) {
      if (generateLimitsMode == true) {
        return super.getOverallLimits(x);
      }
      return limits;
    }

    void setMtabFile(String file) {
      mtabFile = file;
    }

    @Override
    String getMtabFileName() {
      return mtabFile;
    }

    void setGPUAllocator(GPUAllocator gpuAllocator) {
      this.gpuAllocator = gpuAllocator;
    }

    @Override
    GPUAllocator getGPUAllocator() {
      return gpuAllocator;
    }
  }

  @Test
  public void testInit() throws IOException {
    LinuxContainerExecutor mockLCE = new MockLinuxContainerExecutor();
    CustomCgroupsLCEResourceHandler handler =
            new CustomCgroupsLCEResourceHandler();
    YarnConfiguration conf = new YarnConfiguration();
    final int numProcessors = 4;
    ResourceCalculatorPlugin plugin =
            Mockito.mock(ResourceCalculatorPlugin.class);
    Mockito.doReturn(numProcessors).when(plugin).getNumProcessors();
    handler.setConf(conf);
    handler.initConfig();

    // create mock cgroup
    File cgroupMountDirCPU = createMockCgroupMount(cgroupDir, "cpu");
    File cgroupMountDirGPU = createMockCgroupMount(cgroupDir, "devices");

    // create mock mtab
    File mockMtab = createMockMTab(cgroupDir);

    // setup our handler and call init()
    handler.setMtabFile(mockMtab.getAbsolutePath());

    // check values
    // in this case, we're using all cpu so the files
    // shouldn't exist(because init won't create them
    handler.init(mockLCE, plugin);
    File periodFile = new File(cgroupMountDirCPU, "cpu.cfs_period_us");
    File quotaFile = new File(cgroupMountDirCPU, "cpu.cfs_quota_us");
    File devicesAllowFile = new File(cgroupMountDirGPU, "devices.allow");
    File devicesDenyFile = new File(cgroupMountDirGPU, "devices.deny");
    Assert.assertFalse(periodFile.exists());
    Assert.assertFalse(quotaFile.exists());
    Assert.assertFalse(devicesAllowFile.exists());
    Assert.assertFalse(devicesDenyFile.exists());

    // subset of cpu being used, files should be created
    conf
            .setInt(YarnConfiguration.NM_RESOURCE_PERCENTAGE_PHYSICAL_CPU_LIMIT, 75);
    handler.limits[0] = 100 * 1000;
    handler.limits[1] = 1000 * 1000;
    handler.init(mockLCE, plugin);
    int period = readIntFromFile(periodFile);
    int quota = readIntFromFile(quotaFile);
    Assert.assertEquals(100 * 1000, period);
    Assert.assertEquals(1000 * 1000, quota);

    // set cpu back to 100, quota should be -1
    conf.setInt(YarnConfiguration.NM_RESOURCE_PERCENTAGE_PHYSICAL_CPU_LIMIT,
            100);
    handler.limits[0] = 100 * 1000;
    handler.limits[1] = 1000 * 1000;
    handler.init(mockLCE, plugin);
    quota = readIntFromFile(quotaFile);
    Assert.assertEquals(-1, quota);

    FileUtils.deleteQuietly(cgroupDir);
  }

  private int readIntFromFile(File targetFile) throws IOException {
    Scanner scanner = new Scanner(targetFile);
    try {
      return scanner.hasNextInt() ? scanner.nextInt() : -1;
    } finally {
      scanner.close();
    }
  }

  @Test
  public void testGetOverallLimits() {

    int expectedQuota = 1000 * 1000;
    CgroupsLCEResourcesHandler handler = new CgroupsLCEResourcesHandler();

    int[] ret = handler.getOverallLimits(2);
    Assert.assertEquals(expectedQuota / 2, ret[0]);
    Assert.assertEquals(expectedQuota, ret[1]);

    ret = handler.getOverallLimits(2000);
    Assert.assertEquals(expectedQuota, ret[0]);
    Assert.assertEquals(-1, ret[1]);

    int[] params = { 0, -1 };
    for (int cores : params) {
      try {
        handler.getOverallLimits(cores);
        Assert.fail("Function call should throw error.");
      } catch (IllegalArgumentException ie) {
        // expected
      }
    }

    // test minimums
    ret = handler.getOverallLimits(1000 * 1000);
    Assert.assertEquals(1000 * 1000, ret[0]);
    Assert.assertEquals(-1, ret[1]);
  }

  public static File createMockCgroupMount(File parentDir, String type)
          throws IOException {
    return createMockCgroupMount(parentDir, type, "hadoop-yarn");
  }

  public static File createMockMTab(File parentDir) throws IOException {
    String cpuMtabContent =
            "none " + parentDir.getAbsolutePath()
                    + "/cpu cgroup rw,relatime,cpu 0 0\n";
    String devicesMtabContent =
            "none " + parentDir.getAbsolutePath()
                    + "/devices cgroup rw,relatime,devices 0 0\n";

    File mockMtab = new File(parentDir, UUID.randomUUID().toString());
    if (!mockMtab.exists()) {
      if (!mockMtab.createNewFile()) {
        String message = "Could not create file " + mockMtab.getAbsolutePath();
        throw new IOException(message);
      }
    }
    FileWriter mtabWriter = new FileWriter(mockMtab.getAbsoluteFile());
    mtabWriter.write(cpuMtabContent);
    mtabWriter.write(devicesMtabContent);
    mtabWriter.close();
    mockMtab.deleteOnExit();
    return mockMtab;
  }

  @Test
  public void testContainerLimits() throws IOException {
    LinuxContainerExecutor mockLCE = new MockLinuxContainerExecutor();
    CustomCgroupsLCEResourceHandler handler =
            new CustomCgroupsLCEResourceHandler();
    handler.generateLimitsMode = true;
    YarnConfiguration conf = new YarnConfiguration();
    final int numProcessors = 4;
    ResourceCalculatorPlugin plugin =
            Mockito.mock(ResourceCalculatorPlugin.class);
    Mockito.doReturn(numProcessors).when(plugin).getNumProcessors();
    handler.setConf(conf);
    handler.initConfig();

    // create mock cgroup
    File cgroupMountDirCPU = createMockCgroupMount(cgroupDir, "cpu");
    File cgroupMountDirGPU = createMockCgroupMount(cgroupDir, "devices");

    // create mock mtab
    File mockMtab = createMockMTab(cgroupDir);

    // setup our handler and call init()
    handler.setMtabFile(mockMtab.getAbsolutePath());
    handler.init(mockLCE, plugin);

    // check values
    // default case - files shouldn't exist, strict mode off by default
    ContainerId id = ContainerId.fromString("container_1_1_1_1");
    handler.preExecute(id, Resource.newInstance(1024, 1));
    File containerDir = new File(cgroupMountDirCPU, id.toString());
    Assert.assertTrue(containerDir.exists());
    Assert.assertTrue(containerDir.isDirectory());
    File periodFile = new File(containerDir, "cpu.cfs_period_us");
    File quotaFile = new File(containerDir, "cpu.cfs_quota_us");
    Assert.assertFalse(periodFile.exists());
    Assert.assertFalse(quotaFile.exists());

    // no files created because we're using all cpu
    FileUtils.deleteQuietly(containerDir);
    conf.setBoolean(
            YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_STRICT_RESOURCE_USAGE, true);
    handler.initConfig();
    handler.preExecute(id,
            Resource.newInstance(1024, YarnConfiguration.DEFAULT_NM_VCORES));
    Assert.assertTrue(containerDir.exists());
    Assert.assertTrue(containerDir.isDirectory());
    periodFile = new File(containerDir, "cpu.cfs_period_us");
    quotaFile = new File(containerDir, "cpu.cfs_quota_us");
    Assert.assertFalse(periodFile.exists());
    Assert.assertFalse(quotaFile.exists());

    // 50% of CPU
    FileUtils.deleteQuietly(containerDir);
    conf.setBoolean(
            YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_STRICT_RESOURCE_USAGE, true);
    handler.initConfig();
    handler.preExecute(id,
            Resource.newInstance(1024, YarnConfiguration.DEFAULT_NM_VCORES / 2));
    Assert.assertTrue(containerDir.exists());
    Assert.assertTrue(containerDir.isDirectory());
    periodFile = new File(containerDir, "cpu.cfs_period_us");
    quotaFile = new File(containerDir, "cpu.cfs_quota_us");
    Assert.assertTrue(periodFile.exists());
    Assert.assertTrue(quotaFile.exists());
    Assert.assertEquals(500 * 1000, readIntFromFile(periodFile));
    Assert.assertEquals(1000 * 1000, readIntFromFile(quotaFile));

    // CGroups set to 50% of CPU, container set to 50% of YARN CPU
    FileUtils.deleteQuietly(containerDir);
    conf.setBoolean(
            YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_STRICT_RESOURCE_USAGE, true);
    conf
            .setInt(YarnConfiguration.NM_RESOURCE_PERCENTAGE_PHYSICAL_CPU_LIMIT, 50);
    handler.initConfig();
    handler.init(mockLCE, plugin);
    handler.preExecute(id,
            Resource.newInstance(1024, YarnConfiguration.DEFAULT_NM_VCORES / 2));
    Assert.assertTrue(containerDir.exists());
    Assert.assertTrue(containerDir.isDirectory());
    periodFile = new File(containerDir, "cpu.cfs_period_us");
    quotaFile = new File(containerDir, "cpu.cfs_quota_us");
    Assert.assertTrue(periodFile.exists());
    Assert.assertTrue(quotaFile.exists());
    Assert.assertEquals(1000 * 1000, readIntFromFile(periodFile));
    Assert.assertEquals(1000 * 1000, readIntFromFile(quotaFile));

    FileUtils.deleteQuietly(cgroupDir);
  }

  @Test
  public void testContainerGPUAllocation() throws IOException {
    LinuxContainerExecutor mockLCE = new MockLinuxContainerExecutor();
    CustomCgroupsLCEResourceHandler handler =
            new CustomCgroupsLCEResourceHandler();
    YarnConfiguration conf = new YarnConfiguration();
    final int numGPUs = 4;
    ResourceCalculatorPlugin plugin =
            Mockito.mock(ResourceCalculatorPlugin.class);
    Mockito.doReturn(numGPUs).when(plugin).getNumGPUs();
    handler.setConf(conf);
    handler.initConfig();

    // create mock cgroup
    File cgroupMountDirCPU = createMockCgroupMount(cgroupDir, "cpu");
    File cgroupMountDirGPU = createMockCgroupMount(cgroupDir, "devices");

    // create mock mtab
    File mockMtab = createMockMTab(cgroupDir);
    
    ContainerId id = ContainerId.fromString("container_1_1_1_1");
    
    GPUAllocator gpuAllocator = Mockito.mock(GPUAllocator.class);
    Mockito.doReturn(true).when(gpuAllocator).isInitialized();
    HashSet<Device> availableGPUs = new HashSet<>();

    Device gpu0 = new Device(195, 0);
    Device gpu1 = new Device(195, 1);
    Device gpu2 = new Device(195, 2);
    Device gpu3 = new Device(195, 3);

    availableGPUs.add(gpu0);
    availableGPUs.add(gpu1);
    availableGPUs.add(gpu2);
    availableGPUs.add(gpu3);

    Mockito.doReturn(availableGPUs).when(gpuAllocator).getAvailableDevices();
    
    HashSet<Device> mandatoryDevices = new HashSet<>();
    mandatoryDevices.add(new Device(0, 1));
    mandatoryDevices.add(new Device(0, 2));
    Mockito.doReturn(mandatoryDevices).when(gpuAllocator).getMandatoryDevices();

    HashMap<String, HashSet<Device>> map = new HashMap<>();
    HashSet<Device> deniedGPUs = new HashSet<>();
    deniedGPUs.add(gpu0);
    deniedGPUs.add(gpu1);
    map.put("deny", deniedGPUs);
    Mockito.doReturn(map).when(gpuAllocator).allocate(id.toString(), 2);
    
    // setup our handler and call init()
    handler.setGPUAllocator(gpuAllocator);
    handler.setMtabFile(mockMtab.getAbsolutePath());
    handler.init(mockLCE, plugin);
    File containerDirGPU = new File(cgroupMountDirGPU, id.toString());
    File denyFile = new File(containerDirGPU, "devices.deny");
    Assert.assertFalse(denyFile.exists());
    handler.preExecute(id, Resource.newInstance(1024, 1, 2));
    Assert.assertTrue(containerDirGPU.exists());
    Assert.assertTrue(containerDirGPU.isDirectory());
    Assert.assertTrue(denyFile.exists());

    List<String> denyEntries = FileUtils.readLines(denyFile);
    Assert.assertTrue(denyEntries.size() == 2);
    Assert.assertTrue(denyEntries.contains("c " + gpu0 + " rwm"));
    Assert.assertTrue(denyEntries.contains("c " + gpu1 + " rwm"));

    File allowFile = new File(cgroupMountDirGPU, "devices.allow");
    Assert.assertTrue(allowFile.exists());
    List<String> allowEntries = FileUtils.readLines(allowFile);

    Assert.assertTrue(allowEntries.contains("c " + gpu2 + " rwm"));
    Assert.assertTrue(allowEntries.contains("c " + gpu3 + " rwm"));
    
    FileUtils.deleteQuietly(cgroupDir);
  }
  
  private static class CustomGPUmanagementLibrary implements GPUManagementLibrary {
    
    @Override
    public boolean initialize() {
      return true;
    }
    
    @Override
    public boolean shutDown() {
      return true;
    }
    
    @Override
    public int getNumGPUs() {
      return 8;
    }
    
    @Override
    public String queryMandatoryDevices() {
      return "0:1 0:2 0:3";
    }
    
    @Override
    public String queryAvailableDevices(int configuredGPUs) {
      return "195:0 195:1 195:2 195:3 195:4 195:5 195:6 195:7";
    }
  }
  
  @Test
  public void testContainerRecovery() throws IOException {
    LinuxContainerExecutor mockLCE = new MockLinuxContainerExecutor();
    CustomCgroupsLCEResourceHandler handler =
        new CustomCgroupsLCEResourceHandler();
    YarnConfiguration conf = new YarnConfiguration();
    final int numGPUs = 8;
    ResourceCalculatorPlugin plugin =
        Mockito.mock(ResourceCalculatorPlugin.class);
    Mockito.doReturn(numGPUs).when(plugin).getNumGPUs();
    handler.setConf(conf);
    handler.initConfig();
    
    // create mock cgroup
    File cgroupMountDirCPU = createMockCgroupMount(cgroupDir, "cpu");
    File cgroupMountDirGPU = createMockCgroupMount(cgroupDir, "devices");
    
    // create mock mtab
    File mockMtab = createMockMTab(cgroupDir);
    handler.setMtabFile(mockMtab.getAbsolutePath());
    GPUAllocator gpuAllocator = new GPUAllocator(new
        CustomGPUmanagementLibrary(), 8);

    handler.setGPUAllocator(gpuAllocator);
    handler.init(mockLCE, plugin);
    
    ContainerId id1 = ContainerId.fromString("container_1_1_1_1");
    handler.preExecute(id1, Resource.newInstance(1024, 1, 2));
    File containerDir1 = new File(cgroupMountDirGPU, id1.toString());
    File allowFile1 = new File(containerDir1, "devices.allow");
    FileOutputStream fos1 = FileUtils.openOutputStream(allowFile1);
    fos1.write(("c 195:0 rwm\n" + "c 195:1 rwm\n").getBytes());
    
    ContainerId id2 = ContainerId.fromString("container_1_1_1_2");
    handler.preExecute(id2, Resource.newInstance(1024, 1, 2));
    File containerDir2 = new File(cgroupMountDirGPU, id2.toString());
    File allowFile2 = new File(containerDir2, "devices.allow");
    FileOutputStream fos2 = FileUtils.openOutputStream(allowFile2);
    fos2.write(("c 195:2 rwm\n" + "c 195:3 rwm\n").getBytes());
    
    ContainerId id3 = ContainerId.fromString("container_1_1_1_3");
    handler.preExecute(id3, Resource.newInstance(1024, 1, 2));
    File containerDir3 = new File(cgroupMountDirGPU, id3.toString());
    File allowFile3 = new File(containerDir3, "devices.allow");
    FileOutputStream fos3 = FileUtils.openOutputStream(allowFile3);
    fos3.write(("c 195:4 rwm\n" + "c 195:5 rwm\n").getBytes());
  
    gpuAllocator = new GPUAllocator(new
        CustomGPUmanagementLibrary(), 8);
    gpuAllocator.initialize(8);
    handler =
        new CustomCgroupsLCEResourceHandler();
    conf = new YarnConfiguration();
    plugin =
        Mockito.mock(ResourceCalculatorPlugin.class);
    Mockito.doReturn(numGPUs).when(plugin).getNumGPUs();
    handler.setConf(conf);
    handler.initConfig();
    
    handler.setMtabFile(mockMtab.getAbsolutePath());
    handler.setGPUAllocator(gpuAllocator);
    handler.init(mockLCE, plugin);
    
    //No recovery yet
    Assert.assertEquals(8, gpuAllocator.getAvailableDevices().size());
    Assert.assertTrue(allowFile1.exists());
    handler.recoverDeviceControlSystem(id1);
    Assert.assertEquals(6, gpuAllocator.getAvailableDevices().size());
    Assert.assertTrue(allowFile2.exists());
    handler.recoverDeviceControlSystem(id2);
    Assert.assertEquals(4, gpuAllocator.getAvailableDevices().size());
    Assert.assertTrue(allowFile3.exists());
    handler.recoverDeviceControlSystem(id3);
    Assert.assertEquals(2, gpuAllocator.getAvailableDevices().size());
    HashSet<Device> availableGPUs = new HashSet<>(gpuAllocator
        .getAvailableDevices());
    Assert.assertTrue(availableGPUs.contains(new Device(195, 6)));
    Assert.assertTrue(availableGPUs.contains(new Device(195, 7)));
    
    FileUtils.deleteQuietly(cgroupDir);
  }

  @Test
  public void testPrepareDeviceSubsystem() throws IOException {
    LinuxContainerExecutor mockLCE = new MockLinuxContainerExecutor();
    CustomCgroupsLCEResourceHandler handler =
            new CustomCgroupsLCEResourceHandler();
    YarnConfiguration conf = new YarnConfiguration();
    final int numGPUs = 8;
    ResourceCalculatorPlugin plugin =
            Mockito.mock(ResourceCalculatorPlugin.class);
    Mockito.doReturn(numGPUs).when(plugin).getNumGPUs();
    handler.setConf(conf);
    handler.initConfig();

    
    // create mock cgroup
    File cgroupMountDirCPU = createMockCgroupMount(cgroupDir, "cpu");
    File cgroupMountDirGPU = createMockCgroupMount(cgroupDir, "devices");
    
    // create mock mtab
    File mockMtab = createMockMTab(cgroupDir);
    
    GPUAllocator gpuAllocator = new GPUAllocator(new
        CustomGPUmanagementLibrary(), 8);

    // setup our handler and call init()
    handler.setMtabFile(mockMtab.getAbsolutePath());
    handler.setGPUAllocator(gpuAllocator);
    handler.init(mockLCE, plugin);
    
    List<String> cgroupDeviceDirEntries = FileUtils.readLines(new File
        (cgroupMountDirGPU, "devices.allow"), "UTF-8");
    

    for(String defaultDevice : handler.getDefaultWhiteListEntries()) {
      Assert.assertTrue(cgroupDeviceDirEntries.contains(defaultDevice));
    }
    for(Device gpu : gpuAllocator.getAvailableDevices()) {
      Assert.assertTrue(cgroupDeviceDirEntries.contains("c " + gpu.toString()
          + " rwm"));
    }
    for(Device mandatoryDev : gpuAllocator.getMandatoryDevices()) {
      Assert.assertTrue(cgroupDeviceDirEntries.contains("c " + mandatoryDev.toString() + " rwm"));
    }
    FileUtils.deleteQuietly(cgroupDir);
  }
  
  public static File createMockCgroupMount(File parentDir, String type,
                                           String hierarchy) throws IOException {
    File cgroupMountDir =
            new File(parentDir.getAbsolutePath(), type + "/" + hierarchy);
    FileUtils.deleteQuietly(cgroupMountDir);
    if (!cgroupMountDir.mkdirs()) {
      String message =
              "Could not create dir " + cgroupMountDir.getAbsolutePath();
      throw new IOException(message);
    }
    return cgroupMountDir;
  }
}
