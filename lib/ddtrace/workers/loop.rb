module Datadog
  module Workers
    # Adds looping behavior to workers, with a sleep
    # interval between each loop.
    module IntervalLoop
      BACK_OFF_RATIO = 1.2
      BACK_OFF_MAX = 5
      DEFAULT_INTERVAL = 1

      def self.included(base)
        base.send(:prepend, PrependedMethods)
      end

      # Methods that must be prepended
      module PrependedMethods
        def perform(*args)
          perform_loop { super(*args) }
        end
      end

      def perform_loop
        @back_off ||= interval

        loop do
          if work_pending?
            # Run the task
            yield

            # Reset the wait interval
            back_off!(interval)
          elsif back_off?
            # Back off the wait interval a bit
            back_off!
          end

          # Wait for an interval, unless shutdown has been signaled.
          mutex.synchronize do
            return unless run? || work_pending?
            shutdown.wait(mutex, @back_off) if run?
          end
        end
      end

      def stop
        mutex.synchronize do
          return unless run?

          @run = false
          shutdown.signal
        end

        true
      end

      def work_pending?
        # Work is pending by default if the loop is set to run.
        # But defer to any other defined behavior if available.
        defined?(super) ? super : run?
      end

      def run?
        (@run ||= true) == true
      end

      def back_off?
        false
      end

      def back_off!(amount = nil)
        @back_off ||= interval
        @back_off = amount || [@back_off * BACK_OFF_RATIO, BACK_OFF_MAX].min
      end

      protected

      attr_writer \
        :back_off_max,
        :back_off_ratio,
        :buffer,
        :interval

      private

      def interval
        @interval ||= DEFAULT_INTERVAL
      end

      def back_off_ratio
        @back_off_ratio ||= BACK_OFF_RATIO
      end

      def back_off_max
        @back_off_max ||= BACK_OFF_MAX
      end

      def shutdown
        @shutdown ||= ConditionVariable.new
      end
    end
  end
end
