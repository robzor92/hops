package org.apache.hadoop.yarn.server.nodemanager.util;

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

        ContainerId id = ContainerId.fromString("container_1_1_1_1");
        HashMap<String, HashSet<Device>> allocatedGPUs = customGPUAllocator.allocate(id.toString(), 3);
        HashSet<Device> currentlyAvailableDevices = customGPUAllocator.getAvailableDevices();
        Assert.assertEquals(numInitialAvailableGPUs - 3, currentlyAvailableDevices.size());

        HashSet<Device> deniedGPUs = allocatedGPUs.get("deny");
        Assert.assertTrue(currentlyAvailableDevices.equals(deniedGPUs));
        customGPUAllocator.release(id.toString());
        Assert.assertEquals(numInitialAvailableGPUs, customGPUAllocator.getAvailableDevices().size());
    }

    @Test
    public void testGPUAllocatorRecovery() {
        CustomGPUmanagementLibrary lib = new CustomGPUmanagementLibrary();
        CustomGPUAllocator customGPUAllocator = new CustomGPUAllocator(lib);
        customGPUAllocator.initialize();

        HashSet<Device> initialAvailableGPUs = new HashSet<>(customGPUAllocator.getAvailableDevices());
        int numInitialAvailableGPUs = initialAvailableGPUs.size();

        //First container was allocated 195:0 and 195:1
        Device device0 = new Device(195, 0);
        Device device1 = new Device(195, 1);
        ContainerId id = ContainerId.fromString("container_1_1_1_1");
        String devicesAllowStr =
                "c " + device0.toString() + " rwm" +"\n" +
                "c " + device1.toString() + " rwm" + "\n" +
                "c 0:1 rwm\n" +
                "c 0:2 rwm\n";
        customGPUAllocator.recoverAllocation(id.toString(), devicesAllowStr);

        HashSet<Device> availableGPUsAfterFirstRecovery = new HashSet<>(customGPUAllocator.getAvailableDevices());

        Assert.assertFalse(initialAvailableGPUs.equals(availableGPUsAfterFirstRecovery));
        Assert.assertEquals(numInitialAvailableGPUs - 2, availableGPUsAfterFirstRecovery.size());
        Assert.assertFalse(availableGPUsAfterFirstRecovery.contains(device0));
        Assert.assertFalse(availableGPUsAfterFirstRecovery.contains(device1));

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
