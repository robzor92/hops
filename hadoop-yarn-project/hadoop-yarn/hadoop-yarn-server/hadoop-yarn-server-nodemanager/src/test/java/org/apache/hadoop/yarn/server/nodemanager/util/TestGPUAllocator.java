package org.apache.hadoop.yarn.server.nodemanager.util;

import com.google.common.collect.Sets;
import io.hops.GPUManagementLibrary;
import io.hops.devices.Device;
import io.hops.devices.GPUAllocator;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

public class TestGPUAllocator {

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
        public String queryAvailableDevices() {
            return "195:0 195:1 195:2 195:3 195:4 195:5 195:6 195:7";
        }
    }

    private static class CustomGPUAllocator extends GPUAllocator {

        public CustomGPUAllocator(GPUManagementLibrary gpuManagementLibrary) {
            super(gpuManagementLibrary);
        }
    }


    @Test
    public void testGPUAllocation() throws IOException {

        CustomGPUmanagementLibrary lib = new CustomGPUmanagementLibrary();
        CustomGPUAllocator customGPUAllocator = new CustomGPUAllocator(lib);
        customGPUAllocator.initialize();
        HashSet<Device> initialAvailableGPUs = customGPUAllocator.getAvailableDevices();
        int numInitialAvailableGPUs = initialAvailableGPUs.size();

        ContainerId firstId = ContainerId.fromString("container_1_1_1_1");
        HashMap<String, HashSet<Device>> firstAllocation = customGPUAllocator.allocate(firstId.toString(), 4);
        HashSet<Device> currentlyAvailableDevices = new HashSet<>(customGPUAllocator.getAvailableDevices());
        Assert.assertEquals(numInitialAvailableGPUs - 4, currentlyAvailableDevices.size());

        HashSet<Device> deniedGPUs = firstAllocation.get("deny");
        Assert.assertTrue(currentlyAvailableDevices.equals(deniedGPUs));

        ContainerId secondId = ContainerId.fromString("container_1_1_1_2");
        HashMap<String, HashSet<Device>> secondAllocation = customGPUAllocator.allocate(secondId.toString(), 4);
        Assert.assertEquals(4, firstAllocation.get("allow").size());
        Assert.assertEquals(4, firstAllocation.get("deny").size());
        Assert.assertEquals(4, secondAllocation.get("allow").size());
        Assert.assertEquals(4, secondAllocation.get("deny").size());
        Assert.assertEquals(numInitialAvailableGPUs - 8, customGPUAllocator.getAvailableDevices().size());

        customGPUAllocator.release(firstId.toString());
        Assert.assertEquals(numInitialAvailableGPUs - 4, customGPUAllocator.getAvailableDevices().size());

        Assert.assertEquals(0, Sets.intersection(firstAllocation.get("allow"), secondAllocation.get("allow")).size());
        customGPUAllocator.release(secondId.toString());
    }

    @Test
    public void testGPUAllocatorRecovery() throws IOException{
        CustomGPUmanagementLibrary lib = new CustomGPUmanagementLibrary();
        CustomGPUAllocator customGPUAllocator = new CustomGPUAllocator(lib);
        customGPUAllocator.initialize();

        HashSet<Device> initialAvailableGPUs = new HashSet<>(customGPUAllocator.getAvailableDevices());
        int numInitialAvailableGPUs = initialAvailableGPUs.size();

        //First container was allocated 195:0 and 195:1
        Device device0 = new Device(195, 0);
        Device device1 = new Device(195, 1);
        ContainerId firstId = ContainerId.fromString("container_1_1_1_1");
        String firstDevicesAllow =
                "c " + device0.toString() + " rwm" +"\n" +
                "c " + device1.toString() + " rwm" + "\n" +
                "c 0:1 rwm\n" +
                "c 0:2 rwm\n";
        customGPUAllocator.recoverAllocation(firstId.toString(), firstDevicesAllow);

        HashSet<Device> availableGPUsAfterFirstRecovery = new HashSet<>(customGPUAllocator.getAvailableDevices());
        Assert.assertFalse(initialAvailableGPUs.equals(availableGPUsAfterFirstRecovery));
        Assert.assertEquals(numInitialAvailableGPUs - 2, availableGPUsAfterFirstRecovery.size());
        Assert.assertFalse(availableGPUsAfterFirstRecovery.contains(device0));
        Assert.assertFalse(availableGPUsAfterFirstRecovery.contains(device1));

        //First container was allocated 195:2 and 195:3
        Device device2 = new Device(195, 2);
        Device device3 = new Device(195, 3);
        ContainerId id = ContainerId.fromString("container_1_1_1_2");
        String secondDevicesAllow =
                "c " + device2.toString() + " rwm" +"\n" +
                "c " + device3.toString() + " rwm" + "\n" +
                "c 0:1 rwm\n" +
                "c 0:2 rwm\n";
        customGPUAllocator.recoverAllocation(id.toString(), secondDevicesAllow);

        HashSet<Device> availableGPUsAfterSecondRecovery = new HashSet<>(customGPUAllocator.getAvailableDevices());
        Assert.assertFalse(initialAvailableGPUs.equals(availableGPUsAfterSecondRecovery));
        Assert.assertEquals(numInitialAvailableGPUs - 4, availableGPUsAfterSecondRecovery.size());
        Assert.assertFalse(availableGPUsAfterSecondRecovery.contains(device2));
        Assert.assertFalse(availableGPUsAfterSecondRecovery.contains(device3));

        ContainerId newContainerId = ContainerId.fromString("container_1_1_1_3");
        HashMap<String, HashSet<Device>> allocation = customGPUAllocator.allocate(newContainerId.toString(), 4);
        Assert.assertEquals(4, allocation.get("deny").size());
        HashSet<Device> alreadyAllocatedDevices = new HashSet<>();
        alreadyAllocatedDevices.add(device0);
        alreadyAllocatedDevices.add(device1);
        alreadyAllocatedDevices.add(device2);
        alreadyAllocatedDevices.add(device3);
        Assert.assertTrue(allocation.get("deny").containsAll(alreadyAllocatedDevices));

        HashSet<Device> allowedDevices = new HashSet<>();
        Device device4 = new Device(195, 4);
        Device device5 = new Device(195, 5);
        Device device6 = new Device(195, 6);
        Device device7 = new Device(195, 7);

        allowedDevices.add(device4);
        allowedDevices.add(device5);
        allowedDevices.add(device6);
        allowedDevices.add(device7);
        Assert.assertEquals(4, allocation.get("allow").size());
        Assert.assertTrue(allocation.get("allow").containsAll(allowedDevices));
        Assert.assertTrue(customGPUAllocator.getAvailableDevices().isEmpty());
    }

    @Test(expected=IOException.class)
    public void testExceedingGPUResource() throws IOException {

        CustomGPUmanagementLibrary lib = new CustomGPUmanagementLibrary();
        CustomGPUAllocator customGPUAllocator = new CustomGPUAllocator(lib);
        customGPUAllocator.initialize();

        ContainerId firstContainerId = ContainerId.fromString("container_1_1_1_1");
        HashMap<String, HashSet<Device>> firstAllocation = customGPUAllocator.allocate(firstContainerId.toString(), 4);

        //Should throw IOException
        ContainerId secondContainerId = ContainerId.fromString("container_1_1_1_2");
        HashMap<String, HashSet<Device>> secondAllocation = customGPUAllocator.allocate(secondContainerId.toString(), 5);
    }
}
