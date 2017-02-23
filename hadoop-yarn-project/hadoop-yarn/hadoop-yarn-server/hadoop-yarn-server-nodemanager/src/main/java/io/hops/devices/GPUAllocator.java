package io.hops.devices;



import com.google.common.annotations.VisibleForTesting;
import io.hops.GPUManagementLibrary;
import io.hops.GPUManagementLibraryLoader;
import io.hops.exceptions.GPUManagementLibraryException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ContainerId;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GPUAllocator {
  
  private static final GPUAllocator gpuAllocator = new GPUAllocator();
  final static Log LOG = LogFactory.getLog(GPUAllocator.class);

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
          initialize();
    } catch(GPUManagementLibraryException gpue) {
      LOG.info("Could not locate libhops-nvml.so, no GPUs will be offered " +
          "by this NM");
    }

  }

  @VisibleForTesting
  public GPUAllocator(GPUManagementLibrary gpuManagementLibrary) {
    this.gpuManagementLibrary = gpuManagementLibrary;
    this.initialized = true;
  }
  
  /**
   * Initialize the NVML library to check that it was setup correctly
   * @return boolean for success or not
   */
  public boolean initialize() {
    if(initialized == false) {
      initialized = gpuManagementLibrary.initialize();
      initMandatoryDevices();
      initAvailableDevices();
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
  private void initAvailableDevices() {
    String[] availableDeviceIds = gpuManagementLibrary
        .queryAvailableDevices().split(" ");
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
    
    if(availableDevices.size() >= gpus) {
      //Containing entries for GPUs to allow or deny
      HashMap<String, HashSet<Device>> cGroupDeviceMapping = new HashMap<>();
      
      HashSet<Device> currentlyAllocatedGPUs = getAllocatedGPUs();
      //deny devices already in use
      cGroupDeviceMapping.put("deny", currentlyAllocatedGPUs);
      
      HashSet<Device> deviceAllocation = new HashSet<>();
      Iterator<Device> itr = availableDevices.iterator();
      while(gpus != 0) {
        Device gpu = itr.next();
        itr.remove();
        deviceAllocation.add(gpu);
        gpus--;
      }
        
      containerDeviceAllocation.put(containerName, deviceAllocation);
      //only allow access to allocated GPUs
      cGroupDeviceMapping.put("allow", deviceAllocation);
      //need to deny remaining available devices
      cGroupDeviceMapping.get("deny").addAll(availableDevices);
      return cGroupDeviceMapping;
      
    } else {
      throw new IOException("Container " + containerName + " requested " +
          gpus + " GPUs when only " + availableDevices.size() + " available");
    }
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
  private static final Pattern DEVICES_ALLOW_FORMAT = Pattern.compile(
      "([^\\s]+)+\\s([\\d+:\\d]+)+\\s([^\\s]+)");
  
  /**
   * Find GPU devices in the contents of the devices.allow file
   * This method is used in the recovery process and will only filter out the
   * Nvidia GPUs (major device number 195)
   * @param devicesAllowStr
   * @return
   */
  private HashSet<Device> findGPUDevices(String devicesAllowStr) {
    HashSet<Device> devices = new HashSet<>();
  
    Matcher m = DEVICES_ALLOW_FORMAT.matcher(devicesAllowStr);
  
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
