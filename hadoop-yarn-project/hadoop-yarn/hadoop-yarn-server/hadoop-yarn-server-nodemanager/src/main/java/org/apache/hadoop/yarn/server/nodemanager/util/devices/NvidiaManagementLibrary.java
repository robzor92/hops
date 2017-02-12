package org.apache.hadoop.yarn.server.nodemanager.util.devices;

public class NvidiaManagementLibrary {

    static {
      System.out.println(System.getProperty("java.library.path"));
        System.loadLibrary("devices");
    }

    public static native boolean initialize();

    public static native boolean shutDown();

    public static native String queryMandatoryDevices();

    public static native String queryAvailableDevices();
    
    public static void main(String [] args) {
        NvidiaManagementLibrary.initialize();
          System.out.println("mandatory devs: " +
              NvidiaManagementLibrary.queryMandatoryDevices());
          System.out.println("available devs: " +
              NvidiaManagementLibrary.queryAvailableDevices());
          NvidiaManagementLibrary.shutDown();
    }
}
