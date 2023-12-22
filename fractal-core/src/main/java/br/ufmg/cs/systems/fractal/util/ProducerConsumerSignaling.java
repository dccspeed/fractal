package br.ufmg.cs.systems.fractal.util;

abstract public class ProducerConsumerSignaling {
   private final Object producerMonitor = new Object();
   private boolean workProduced = false;

   private final Object consumerMonitor = new Object();
   private boolean workConsumed = false;

   // called by producer
   public void notifyWorkProduced() {
      synchronized (producerMonitor) {
         workProduced = true;
         producerMonitor.notify();
      }
   }

   // called by consumer
   public void waitWorkProduced() {
      synchronized (producerMonitor) {
         while (!workProduced) {
            try {
               producerMonitor.wait();
            } catch (InterruptedException e) {
               e.printStackTrace();
            }
         }

         workProduced = false;
      }
   }

   // called by consumer
   public void notifyWorkConsumed() {
      synchronized (consumerMonitor) {
         workConsumed = true;
         consumerMonitor.notify();
      }
   }

   // called by producer
   public void waitWorkConsumed() {
      synchronized (consumerMonitor) {
         while (!workConsumed) {
            try {
               consumerMonitor.wait();
            } catch (InterruptedException e) {
               e.printStackTrace();
            }
         }

         workConsumed = false;
      }
   }
}