/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;

import io.hops.devices.Device;
import io.hops.devices.GPUAllocator;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.LinuxContainerExecutor;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.ResourceCalculatorPlugin;
import org.apache.hadoop.yarn.util.SystemClock;

public class CgroupsLCEResourcesHandler implements LCEResourcesHandler {
  
  final static Log LOG = LogFactory
      .getLog(CgroupsLCEResourcesHandler.class);
  
  private Configuration conf;
  private String cgroupPrefix;
  private boolean cgroupMount;
  private String cgroupMountPath;
  
  private boolean cpuWeightEnabled = true;
  private boolean strictResourceUsageMode = false;
  
  private boolean gpuSupportEnabled = false;
  private static GPUAllocator gpuAllocator;
  
  private final String MTAB_FILE = "/proc/mounts";
  private final String CGROUPS_FSTYPE = "cgroup";
  private final String CONTROLLER_CPU = "cpu";
  private final String CONTROLLER_DEVICES = "devices";
  private final String CPU_PERIOD_US = "cfs_period_us";
  private final String CPU_QUOTA_US = "cfs_quota_us";
  private final int CPU_DEFAULT_WEIGHT = 1024; // set by kernel
  private final int MAX_QUOTA_US = 1000 * 1000;
  private final int MIN_PERIOD_US = 1000;
  private final Map<String, String> controllerPaths; // Controller -> path
  
  private long deleteCgroupTimeout;
  private long deleteCgroupDelay;
  // package private for testing purposes
  Clock clock;
  
  private float yarnProcessors;
  
  public CgroupsLCEResourcesHandler() {
    this.controllerPaths = new HashMap<>();
    clock = new SystemClock();
    
    gpuAllocator = GPUAllocator.getInstance();
    gpuSupportEnabled = gpuAllocator.isInitialized();
    
  }
  
  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }
  
  @Override
  public Configuration getConf() {
    return conf;
  }
  
  @VisibleForTesting
  void initConfig() throws IOException {
    
    this.cgroupPrefix = conf.get(YarnConfiguration.
        NM_LINUX_CONTAINER_CGROUPS_HIERARCHY, "/hadoop-yarn");
    this.cgroupMount = conf.getBoolean(YarnConfiguration.
        NM_LINUX_CONTAINER_CGROUPS_MOUNT, false);
    this.cgroupMountPath = conf.get(YarnConfiguration.
        NM_LINUX_CONTAINER_CGROUPS_MOUNT_PATH, null);
    
    this.deleteCgroupTimeout = conf.getLong(
        YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_DELETE_TIMEOUT,
        YarnConfiguration.DEFAULT_NM_LINUX_CONTAINER_CGROUPS_DELETE_TIMEOUT);
    this.deleteCgroupDelay =
        conf.getLong(YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_DELETE_DELAY,
            YarnConfiguration.DEFAULT_NM_LINUX_CONTAINER_CGROUPS_DELETE_DELAY);
    // remove extra /'s at end or start of cgroupPrefix
    if (cgroupPrefix.charAt(0) == '/') {
      cgroupPrefix = cgroupPrefix.substring(1);
    }
    
    this.strictResourceUsageMode =
        conf
            .getBoolean(
                YarnConfiguration.NM_LINUX_CONTAINER_CGROUPS_STRICT_RESOURCE_USAGE,
                YarnConfiguration.DEFAULT_NM_LINUX_CONTAINER_CGROUPS_STRICT_RESOURCE_USAGE);
    
    int len = cgroupPrefix.length();
    if (cgroupPrefix.charAt(len - 1) == '/') {
      cgroupPrefix = cgroupPrefix.substring(0, len - 1);
    }
  }
  
  public void init(LinuxContainerExecutor lce) throws IOException {
    this.init(lce,
        ResourceCalculatorPlugin.getResourceCalculatorPlugin(null, conf));
  }
  
  @VisibleForTesting
  void init(LinuxContainerExecutor lce, ResourceCalculatorPlugin plugin)
      throws IOException {
    initConfig();
    
    // mount cgroups if requested
    if (cgroupMount && cgroupMountPath != null) {
      ArrayList<String> cgroupKVs = new ArrayList<String>();
      cgroupKVs.add(CONTROLLER_CPU + "=" + cgroupMountPath + "/" +
          CONTROLLER_CPU);
      cgroupKVs.add(CONTROLLER_DEVICES + "=" + cgroupMountPath + "/" +
          CONTROLLER_DEVICES);
      lce.mountCgroups(cgroupKVs, cgroupPrefix);
    }
    
    initializeControllerPaths();
    
    // cap overall usage to the number of cores allocated to YARN
    yarnProcessors = NodeManagerHardwareUtils.getContainersCores(plugin, conf);
    int systemProcessors = plugin.getNumProcessors();
    if (systemProcessors != (int) yarnProcessors) {
      LOG.info("YARN containers restricted to " + yarnProcessors + " cores");
      int[] limits = getOverallLimits(yarnProcessors);
      updateCgroup(CONTROLLER_CPU, "", CPU_PERIOD_US, String.valueOf(limits[0]));
      updateCgroup(CONTROLLER_CPU, "", CPU_QUOTA_US, String.valueOf(limits[1]));
    } else if (cpuLimitsExist()) {
      LOG.info("Removing CPU constraints for YARN containers.");
      updateCgroup(CONTROLLER_CPU, "", CPU_QUOTA_US, String.valueOf(-1));
    }
  }
  
  boolean cpuLimitsExist() throws IOException {
    String path = pathForCgroup(CONTROLLER_CPU, "");
    File quotaFile = new File(path, CONTROLLER_CPU + "." + CPU_QUOTA_US);
    if (quotaFile.exists()) {
      String contents = FileUtils.readFileToString(quotaFile, "UTF-8");
      int quotaUS = Integer.parseInt(contents.trim());
      if (quotaUS != -1) {
        return true;
      }
    }
    return false;
  }
  
  @VisibleForTesting
  int[] getOverallLimits(float yarnProcessors) {
    
    int[] ret = new int[2];
    
    if (yarnProcessors < 0.01f) {
      throw new IllegalArgumentException("Number of processors can't be <= 0.");
    }
    
    int quotaUS = MAX_QUOTA_US;
    int periodUS = (int) (MAX_QUOTA_US / yarnProcessors);
    if (yarnProcessors < 1.0f) {
      periodUS = MAX_QUOTA_US;
      quotaUS = (int) (periodUS * yarnProcessors);
      if (quotaUS < MIN_PERIOD_US) {
        LOG
            .warn("The quota calculated for the cgroup was too low. The minimum value is "
                + MIN_PERIOD_US + ", calculated value is " + quotaUS
                + ". Setting quota to minimum value.");
        quotaUS = MIN_PERIOD_US;
      }
    }
    
    // cfs_period_us can't be less than 1000 microseconds
    // if the value of periodUS is less than 1000, we can't really use cgroups
    // to limit cpu
    if (periodUS < MIN_PERIOD_US) {
      LOG
          .warn("The period calculated for the cgroup was too low. The minimum value is "
              + MIN_PERIOD_US + ", calculated value is " + periodUS
              + ". Using all available CPU.");
      periodUS = MAX_QUOTA_US;
      quotaUS = -1;
    }
    
    ret[0] = periodUS;
    ret[1] = quotaUS;
    return ret;
  }
  
  boolean isCpuWeightEnabled() {
    return this.cpuWeightEnabled;
  }
  
  boolean isGpuSupportEnabled() { return this.gpuSupportEnabled; }

  /*
   * Next four functions are for an individual cgroup.
   */
  
  private String pathForCgroup(String controller, String groupName) {
    String controllerPath = controllerPaths.get(controller);
    return controllerPath + "/" + cgroupPrefix + "/" + groupName;
  }
  
  private void createCgroup(String controller, String groupName)
      throws IOException {
    String path = pathForCgroup(controller, groupName);
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("createCgroup: " + path);
    }
    
    if (! new File(path).mkdir()) {
      throw new IOException("Failed to create cgroup at " + path);
    }
  }
  
  private void updateCgroup(String controller, String groupName, String param,
      String value) throws IOException {
    String path = pathForCgroup(controller, groupName);
    param = controller + "." + param;
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("updateCgroup: " + path + ": " + param + "=" + value);
    }
    
    PrintWriter pw = null;
    try {
      File file = new File(path + "/" + param);
      Writer w = new OutputStreamWriter(new FileOutputStream(file), "UTF-8");
      pw = new PrintWriter(w);
      pw.write(value);
    } catch (IOException e) {
      throw new IOException("Unable to set " + param + "=" + value +
          " for cgroup at: " + path, e);
    } finally {
      if (pw != null) {
        boolean hasError = pw.checkError();
        pw.close();
        if(hasError) {
          throw new IOException("Unable to set " + param + "=" + value +
              " for cgroup at: " + path);
        }
        if(pw.checkError()) {
          throw new IOException("Error while closing cgroup file " + path);
        }
      }
    }
  }
  
  /*
   * Utility routine to print first line from cgroup tasks file
   */
  private void logLineFromTasksFile(File cgf) {
    String str;
    if (LOG.isDebugEnabled()) {
      try (BufferedReader inl =
               new BufferedReader(new InputStreamReader(new FileInputStream(cgf
                   + "/tasks"), "UTF-8"))) {
        if ((str = inl.readLine()) != null) {
          LOG.debug("First line in cgroup tasks file: " + cgf + " " + str);
        }
      } catch (IOException e) {
        LOG.warn("Failed to read cgroup tasks file. ", e);
      }
    }
  }
  
  /**
   * If tasks file is empty, delete the cgroup.
   *
   * @param cgf object referring to the cgroup to be deleted
   * @return Boolean indicating whether cgroup was deleted
   */
  @VisibleForTesting
  boolean checkAndDeleteCgroup(File cgf) throws InterruptedException {
    boolean deleted = false;
    // FileInputStream in = null;
    try (FileInputStream in = new FileInputStream(cgf + "/tasks")) {
      if (in.read() == -1) {
        /*
         * "tasks" file is empty, sleep a bit more and then try to delete the
         * cgroup. Some versions of linux will occasionally panic due to a race
         * condition in this area, hence the paranoia.
         */
        Thread.sleep(deleteCgroupDelay);
        deleted = cgf.delete();
        if (!deleted) {
          LOG.warn("Failed attempt to delete cgroup: " + cgf);
        }
      } else {
        logLineFromTasksFile(cgf);
      }
    } catch (IOException e) {
      LOG.warn("Failed to read cgroup tasks file. ", e);
    }
    return deleted;
  }
  
  @VisibleForTesting
  boolean deleteCgroup(String cgroupPath) {
    boolean deleted = false;
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("deleteCgroup: " + cgroupPath);
    }
    long start = clock.getTime();
    do {
      try {
        deleted = checkAndDeleteCgroup(new File(cgroupPath));
        if (!deleted) {
          Thread.sleep(deleteCgroupDelay);
        }
      } catch (InterruptedException ex) {
        // NOP
      }
    } while (!deleted && (clock.getTime() - start) < deleteCgroupTimeout);
    
    if (!deleted) {
      LOG.warn("Unable to delete cgroup at: " + cgroupPath +
          ", tried to delete for " + deleteCgroupTimeout + "ms");
    }
    return deleted;
  }
  
  private String createCgroupDeviceEntry(HashSet devices) {
    StringBuilder cgroupDeviceEntries = new StringBuilder();
    Iterator<Device> itr = devices.iterator();
    while(itr.hasNext()) {
      cgroupDeviceEntries.append("c " + itr.next().toString() + " rwm\n");
    }
    return cgroupDeviceEntries.toString();
  }

  /*
   * Next three functions operate on all the resources we are enforcing.
   */
  
  private void setupLimits(ContainerId containerId,
      Resource containerResource) throws IOException {
    String containerName = containerId.toString();

    System.out.println("requesting" + containerResource.getGPUs() + " gpus") ;
    
    if (isCpuWeightEnabled()) {
      int containerVCores = containerResource.getVirtualCores();
      createCgroup(CONTROLLER_CPU, containerName);
      int cpuShares = CPU_DEFAULT_WEIGHT * containerVCores;
      updateCgroup(CONTROLLER_CPU, containerName, "shares",
          String.valueOf(cpuShares));
      if (strictResourceUsageMode) {
        int nodeVCores =
            conf.getInt(YarnConfiguration.NM_VCORES,
                YarnConfiguration.DEFAULT_NM_VCORES);
        if (nodeVCores != containerVCores) {
          float containerCPU =
              (containerVCores * yarnProcessors) / (float) nodeVCores;
          int[] limits = getOverallLimits(containerCPU);
          updateCgroup(CONTROLLER_CPU, containerName, CPU_PERIOD_US,
              String.valueOf(limits[0]));
          updateCgroup(CONTROLLER_CPU, containerName, CPU_QUOTA_US,
              String.valueOf(limits[1]));
        }
      }
    }
    
    int containerGPUs = 2;
    if(isGpuSupportEnabled() && containerGPUs > 0) {
      createCgroup(CONTROLLER_DEVICES, containerName);

      
      HashMap<String, HashSet<Device>> cGroupDeviceAccess =
          gpuAllocator.allocate(containerName, containerGPUs);
      
      HashSet<Device> deniedDevices = cGroupDeviceAccess.get("deny");
      
      String cgroupGPUDenyEntries = createCgroupDeviceEntry(deniedDevices);
      updateCgroup(CONTROLLER_DEVICES, containerName, "deny",
          cgroupGPUDenyEntries);
      
      HashSet<Device> allowedDevices = cGroupDeviceAccess.get("allow");
      HashSet<Device> mandatoryDevices = gpuAllocator.getMandatoryDevices();
      
      String cgroupAllowedDevices = createCgroupDeviceEntry(allowedDevices);
      String cgroupMandatoryDevices = createCgroupDeviceEntry(mandatoryDevices);
      
      updateCgroup(CONTROLLER_DEVICES, containerName, "allow",
          cgroupAllowedDevices + cgroupMandatoryDevices);
    }
  }
  
  private void clearLimits(ContainerId containerId) {
    if (isCpuWeightEnabled()) {
      deleteCgroup(pathForCgroup(CONTROLLER_CPU, containerId.toString()));
    }
    
    if(isGpuSupportEnabled()) {
      deleteCgroup(pathForCgroup(CONTROLLER_DEVICES, containerId.toString()));
      gpuAllocator.release(containerId.toString());
    }
  }

  /*
   * LCE Resources Handler interface
   */
  
  public void preExecute(ContainerId containerId, Resource containerResource)
      throws IOException {
    System.out.println(containerResource.getMemory());
    setupLimits(containerId, containerResource);
  }
  
  public void postExecute(ContainerId containerId) {
    clearLimits(containerId);
  }
  
  public String getResourcesOption(ContainerId containerId) {
    String containerName = containerId.toString();
    
    StringBuilder sb = new StringBuilder("cgroups=");
    
    if (isCpuWeightEnabled()) {
      sb.append(pathForCgroup(CONTROLLER_CPU, containerName) + "/tasks");
      sb.append(",");
    }
    if (isGpuSupportEnabled()) {
      sb.append(pathForCgroup(CONTROLLER_DEVICES, containerName) + "/tasks");
      sb.append(",");
    }
    
    if (sb.charAt(sb.length() - 1) == ',') {
      sb.deleteCharAt(sb.length() - 1);
    }
    
    return sb.toString();
  }
  
  @Override
  public void recoverDeviceControlSystem() {
    if(!gpuSupportEnabled) {
      return;
    }
    
    String controllerPath = controllerPaths.get(CONTROLLER_DEVICES);
    String deviceCgroupPath = controllerPath + "/" + cgroupPrefix;
    
    File directory = new File(deviceCgroupPath);
    File[] containers = directory.listFiles((FileFilter) DirectoryFileFilter
        .DIRECTORY);
    for (File container : containers) {
      try {
        String allowFileContents = FileUtils.readFileToString(new File
            (container.getAbsolutePath() + "devices.allow", "UTF-8"));
        gpuAllocator.recoverAllocation(container.getName(), allowFileContents);
      } catch (IOException e1) {
        LOG.error("Could not retrieve contents of file in path " + container
            .getAbsolutePath() + "devices.allow");
      }
    }
  }


  /* We are looking for entries of the form:
   * none /cgroup/path/mem cgroup rw,memory 0 0
   *
   * Use a simple pattern that splits on the five spaces, and
   * grabs the 2, 3, and 4th fields.
   */
  
  private static final Pattern MTAB_FILE_FORMAT = Pattern.compile(
      "^[^\\s]+\\s([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\\s[^\\s]+\\s[^\\s]+$");
  
  /*
   * Returns a map: path -> mount options
   * for mounts with type "cgroup". Cgroup controllers will
   * appear in the list of options for a path.
   */
  private Map<String, List<String>> parseMtab() throws IOException {
    Map<String, List<String>> ret = new HashMap<String, List<String>>();
    BufferedReader in = null;
    
    try {
      FileInputStream fis = new FileInputStream(new File(getMtabFileName()));
      in = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
      
      for (String str = in.readLine(); str != null;
           str = in.readLine()) {
        System.out.println("entry " + str);
        Matcher m = MTAB_FILE_FORMAT.matcher(str);
        boolean mat = m.find();
        if (mat) {
          String path = m.group(1);
          String type = m.group(2);
          String options = m.group(3);
          if (type.equals(CGROUPS_FSTYPE)) {
            List<String> value = Arrays.asList(options.split(","));
            ret.put(path, value);
          }
        }
      }
    } catch (IOException e) {
      throw new IOException("Error while reading " + getMtabFileName(), e);
    } finally {
      IOUtils.cleanup(LOG, in);
    }
    
    return ret;
  }
  
  private String findControllerInMtab(String controller,
      Map<String, List<String>> entries) {
    for (Entry<String, List<String>> e : entries.entrySet()) {
      if (e.getValue().contains(controller))
        return e.getKey();
    }
    
    return null;
  }
  
  private void initializeControllerPaths() throws IOException {
    String cpuControllerPath;
    String devicesControllerPath;
    Map<String, List<String>> parsedMtab = parseMtab();
    
    // CPU
    
    cpuControllerPath = findControllerInMtab(CONTROLLER_CPU, parsedMtab);
    
    if (cpuControllerPath != null) {
      File f = new File(cpuControllerPath + "/" + this.cgroupPrefix);
      if (FileUtil.canWrite(f)) {
        controllerPaths.put(CONTROLLER_CPU, cpuControllerPath);
      } else {
        throw new IOException("Not able to enforce cpu weights; cannot write "
            + "to cgroup at: " + cpuControllerPath);
      }
    } else {
      throw new IOException("Not able to enforce cpu weights; cannot find "
          + "cgroup for cpu controller in " + getMtabFileName());
    }
    
    // GPU
    
    devicesControllerPath = findControllerInMtab(CONTROLLER_DEVICES, parsedMtab);
    
    if (devicesControllerPath != null) {
      File f = new File(devicesControllerPath + "/" + this.cgroupPrefix);
      
      if (FileUtil.canWrite(f)) {
        controllerPaths.put(CONTROLLER_DEVICES, devicesControllerPath);
      } else {
        throw new IOException("Not able to restrict device access; cannot write "
            + "to cgroup at: " + devicesControllerPath);
      }
    } else {
      throw new IOException("Not able to restrict device access; cannot find "
          + "cgroup for devices controller in " + getMtabFileName());
    }
  }
  
  @VisibleForTesting
  String getMtabFileName() {
    return MTAB_FILE;
  }
}
