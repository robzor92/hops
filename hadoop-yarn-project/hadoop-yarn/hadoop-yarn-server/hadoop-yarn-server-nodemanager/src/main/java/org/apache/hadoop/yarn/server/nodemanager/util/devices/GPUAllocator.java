package org.apache.hadoop.yarn.server.nodemanager.util.devices;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

public class GPUAllocator {
  
  private HashSet<Device> availableDevices;
  private HashMap<String, HashSet<Device>> containerDeviceAllocation;
  private HashSet<Device> mandatoryDevices;
  
  public GPUAllocator() {
    availableDevices = new HashSet<>();
    containerDeviceAllocation = new HashMap<>();
    mandatoryDevices = new HashSet<>();
    
    initMandatoryDevices();
    initAvailableDevices();
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
    String[] mandatoryDeviceIds = NvidiaManagementLibrary.getMandatoryDeviceIds();
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
   *
   */
  //TODO pattern match validator, expecting device numbers to be on form of (major:minor)
  private void initAvailableDevices() {
    String[] availableDeviceIds = NvidiaManagementLibrary.getAvailableDeviceIds();
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
   * @return mandatory devices for containers making use of gpus
   */
  public HashSet<Device> getMandatoryDevices() {
    return mandatoryDevices;
  }
  
  /**
   * Finds out which gpus are currently allocated to a container
   *
   * @return HashSet containing devices currently allocated to containers
   */
  public HashSet<Device> getAllocatedGPUs() {
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
    HashSet<Device> deviceAllocation = containerDeviceAllocation.get(containerName);
    availableDevices.addAll(deviceAllocation);
    containerDeviceAllocation.remove(containerName);
  }
}
