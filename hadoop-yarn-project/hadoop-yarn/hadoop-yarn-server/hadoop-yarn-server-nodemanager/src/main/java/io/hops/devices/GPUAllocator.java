package io.hops.devices;



import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import io.hops.GPUManagementLibrary;
import io.hops.GPUManagementLibraryLoader;
import io.hops.exceptions.GPUManagementLibraryException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ContainerId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GPUAllocator {
  
  final static Log LOG = LogFactory.getLog(GPUAllocator.class);
  private static final GPUAllocator gpuAllocator = new GPUAllocator();
  
  private HashSet<Device> availableDevices;
  private HashMap<String, HashSet<Device>> containerDeviceAllocation;
  private HashSet<Device> mandatoryDevices;
  private GPUManagementLibrary gpuManagementLibrary;
  private static final String GPU_MANAGEMENT_LIBRARY_CLASSNAME = "io.hops" +
      ".management.nvidia.NvidiaManagementLibrary";
  private static final int NVIDIA_GPU_MAJOR_DEVICE_NUMBER = 195;
  private boolean initialized = false;
  
  public static GPUAllocator getInstance(){
    return gpuAllocator;
  }
  
  private GPUAllocator() {
    availableDevices = new HashSet<>();
    containerDeviceAllocation = new HashMap<>();
    mandatoryDevices = new HashSet<>();
    
    try {
      gpuManagementLibrary =
          GPUManagementLibraryLoader.load(GPU_MANAGEMENT_LIBRARY_CLASSNAME);
    } catch(GPUManagementLibraryException | UnsatisfiedLinkError e) {
      LOG.error("Could not load GPU management library. Is this NodeManager " +
          "supposed to offer its GPUs as a resource? If yes, check " +
          "installation" +
          " setup and make sure libnvidia-ml.so.1 is present in " +
          "LD_LIBRARY_PATH. If no, disable GPUs by setting yarn.nodemanager" +
          ".resource.gpus.enabled to " +
          "false." + "\n");
      
      e.printStackTrace();
    }
  }
  
  @VisibleForTesting
  public GPUAllocator(GPUManagementLibrary gpuManagementLibrary, int
      configuredGPUs) {
    availableDevices = new HashSet<>();
    containerDeviceAllocation = new HashMap<>();
    mandatoryDevices = new HashSet<>();
    
    this.gpuManagementLibrary = gpuManagementLibrary;
    initialize(configuredGPUs);
  }
  
  /**
   * Initialize the NVML library to check that it was setup correctly
   * @return boolean for success or not
   */
  public boolean initialize(int configuredGPUs) {
    if(initialized == false) {
      initialized = gpuManagementLibrary.initialize();
      initAvailableDevices(configuredGPUs);
      initMandatoryDevices();
    }
    return this.initialized;
  }
  
  public boolean isInitialized() {
    return this.initialized;
  }
  
  /**
   * Shut down NVML by releasing all GPU resources previously allocated with nvmlInit()
   * @return boolean for success or not
   */
  public boolean shutDown() {
    if(initialized) {
      return gpuManagementLibrary.shutDown();
    }
    return false;
  }
  
  /**
   * Queries NVML to discover device numbers for mandatory driver devices that
   * all GPU containers should have access to
   *
   * Mandatory devices for NVIDIA GPUs are:
   * /dev/nvidiactl
   * /dev/nvidia-uvm
   * /dev/nvidia-uvm-tools
   */
  //TODO pattern match validator, expecting device numbers to be on form of (major:minor)
  private void initMandatoryDevices() {
    String[] mandatoryDeviceIds = gpuManagementLibrary
        .queryMandatoryDevices().split(" ");
    for(int i = 0; i < mandatoryDeviceIds.length; i++) {
      String[] majorMinorPair = mandatoryDeviceIds[i].split(":");
      mandatoryDevices.add(new Device(
          Integer.parseInt(majorMinorPair[0]),
          Integer.parseInt(majorMinorPair[1])));
    }
  }
  
  /**
   * Queries NVML to discover device numbers for available Nvidia gpus that
   * may be scheduled and isolated for containers
   */
  //TODO pattern match validator, expecting device numbers to be on form of (major:minor)
  private void initAvailableDevices(int configuredGPUs) {
    String[] availableDeviceIds = gpuManagementLibrary
        .queryAvailableDevices(configuredGPUs).split(" ");
    for(int i = 0; i < availableDeviceIds.length; i++) {
      String[] majorMinorPair = availableDeviceIds[i].split(":");
      availableDevices.add(new Device(Integer.parseInt
          (majorMinorPair[0]), Integer.parseInt(majorMinorPair[1])));
    }
  }
  
  /**
   * Returns the mandatory devices that all containers making use of Nvidia
   * gpus should be given access to
   *
   * @return HashSet containing mandatory devices for containers making use of
   * gpus
   */
  public HashSet<Device> getMandatoryDevices() {
    return mandatoryDevices;
  }
  
  public HashSet<Device> getAvailableDevices() { return availableDevices; }
  
  /**
   * Finds out which gpus are currently allocated to a container
   *
   * @return HashSet containing devices currently allocated to containers
   */
  private HashSet<Device> getAllocatedGPUs() {
    HashSet<Device> allocatedGPUs = new HashSet<>();
    Collection<HashSet<Device>> gpuSets = containerDeviceAllocation.values();
    Iterator<HashSet<Device>> itr = gpuSets.iterator();
    while(itr.hasNext()) {
      HashSet<Device> allocatedGpuSet = itr.next();
      allocatedGPUs.addAll(allocatedGpuSet);
    }
    return allocatedGPUs;
  }
  
  /**
   * The allocation method will allocate the requested number of gpu devices.
   * In order to isolate a set of gpus, it is crucial to only allow device
   * access to those particular gpus and deny access to the remaining
   * devices, whether they are currently allocated to some container or not
   *
   * @param containerName the container to allocate gpus for
   * @param gpus the number of gpus to allocate for container
   * @return HashMap containing mapping for "allow" and "deny" corresponding to
   * which gpu devices should be given access to and which should be denied
   * @throws IOException
   */
  public synchronized HashMap<String, HashSet<Device>> allocate(String
      containerName, int gpus)
      throws IOException {
    LOG.info("Trying to allocate " + gpus + " gpus");
    LOG.info("Currently unallocated gpus: " + availableDevices.toString());
    HashSet<Device> currentlyAllocatedGPUs = getAllocatedGPUs();
    LOG.info("Currently allocated gpus: " + currentlyAllocatedGPUs);
    
    if(availableDevices.size() >= gpus) {
      //Containing entries for GPUs to allow or deny
      HashMap<String, HashSet<Device>> cGroupDeviceMapping = new HashMap<>();
      
      //deny devices already in use
      cGroupDeviceMapping.put("deny", currentlyAllocatedGPUs);
      
      HashSet<Device> deviceAllocation = selectDevicesToAllocate(gpus);
      
      LOG.info("Gpus to allocate for " + containerName + " = " +
          deviceAllocation);
      
      containerDeviceAllocation.put(containerName, deviceAllocation);
      //only allow access to allocated GPUs
      cGroupDeviceMapping.put("allow", deviceAllocation);
      
      //deny remaining available devices
      cGroupDeviceMapping.get("deny").addAll(availableDevices);
      LOG.info("Gpus to deny for " + containerName + " = " +
          cGroupDeviceMapping.get("deny"));
      return cGroupDeviceMapping;
      
    } else {
      throw new IOException("Container " + containerName + " requested " +
          gpus + " GPUs when only " + availableDevices.size() + " available");
    }
  }
  
  /**
   * This method selects GPUs to allocate based on a "Fastest GPU first" policy
   * Device compute capability of NVIDIA GPUs are sorted based on the
   * minor device number. - "The lower the number the faster the device.
   *
   * TODO: In the future this method could support different selection policies
   * TODO: such as "Random GPU", "Slowest GPU first" or something
   * TODO: actually useful like topology-based selection with GPUs
   * TODO: interconnected using NVLink.
   * @param gpus
   * @return set of GPU devices that have been allocated
   */
  private synchronized HashSet<Device> selectDevicesToAllocate(int gpus) {
    
    HashSet<Device> deviceAllocation = new HashSet<>();
    Iterator<Device> availableDeviceItr = availableDevices.iterator();
    
    TreeSet<Integer> minDeviceNums = new TreeSet<>();
    
    while(availableDeviceItr.hasNext()) {
      minDeviceNums.add(availableDeviceItr.next().getMinorDeviceNumber());
    }
    
    Iterator<Integer> minDeviceNumItr = minDeviceNums.iterator();
    
    while(minDeviceNumItr.hasNext() && gpus != 0) {
      int deviceNum = minDeviceNumItr.next();
      Device allocatedGPU = new Device(NVIDIA_GPU_MAJOR_DEVICE_NUMBER, deviceNum);
      availableDevices.remove(allocatedGPU);
      deviceAllocation.add(allocatedGPU);
      gpus--;
    }
    
    return deviceAllocation;
  }
  
  /**
   * Releases gpus for the given container and allows them to be subsequently
   * allocated by other containers
   *
   * @param containerName
   */
  public synchronized void release(String containerName) {
    HashSet<Device> deviceAllocation = containerDeviceAllocation.
        get(containerName);
    availableDevices.addAll(deviceAllocation);
    containerDeviceAllocation.remove(containerName);
  }
  
  /**
   * Given the containerId and the Cgroup contents for devices.allow
   * extract allocated GPU devices
   * @param devicesAllowStr
   */
  public synchronized void recoverAllocation(String containerId, String
      devicesAllowStr) {
    HashSet<Device> allocatedGPUsForContainer = findGPUDevices(devicesAllowStr);
    availableDevices.removeAll(allocatedGPUsForContainer);
    containerDeviceAllocation.put(containerId.toString(),
        allocatedGPUsForContainer);
  }
  
  /* We are looking for entries of the form:
   * c 195:0 rwm
   *
   * Use a simple pattern that splits on the two spaces, and
   * grabs the 2nd field
   */
  private static final Pattern DEVICES_LIST_FORMAT = Pattern.compile(
      "([^\\s]+)+\\s([\\d+:\\d]+)+\\s([^\\s]+)");
  
  /**
   * Find GPU devices in the contents of the devices.list file
   * This method is used in the recovery process and will only filter out the
   * NVIDIA GPUs (major device number 195)
   * @param devicesAllowStr
   * @return
   */
  private HashSet<Device> findGPUDevices(String devicesAllowStr) {
    HashSet<Device> devices = new HashSet<>();
    
    Matcher m = DEVICES_LIST_FORMAT.matcher(devicesAllowStr);
    
    while (m.find()) {
      String majorMinorDeviceNumber = m.group(2);
      String[] majorMinorPair = majorMinorDeviceNumber.split(":");
      int majorDeviceNumber = Integer.parseInt(majorMinorPair[0]);
      if(majorDeviceNumber == NVIDIA_GPU_MAJOR_DEVICE_NUMBER) {
        int minorDeviceNumber = Integer.parseInt(majorMinorPair[1]);
        devices.add(new Device(majorDeviceNumber, minorDeviceNumber));
      }
    }
    return devices;
  }
}
