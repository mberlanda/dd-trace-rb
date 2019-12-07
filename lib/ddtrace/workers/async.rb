require 'ddtrace/logger'

module Datadog
  module Workers
    module Async
      # Adds threading behavior to workers
      # to run tasks asynchronously.
      module Thread
        FORK_POLICY_STOP = :stop
        FORK_POLICY_RESTART = :restart

        def self.included(base)
          base.send(:prepend, PrependedMethods)
        end

        # Methods that must be prepended
        module PrependedMethods
          def perform(*args)
            start { self.result = super(*args) } if unstarted?
          end
        end

        attr_reader \
          :error,
          :result

        attr_writer \
          :fork_policy

        def join(timeout = nil)
          worker.join(timeout)
        end

        def run?
          (@run ||= false) == true
        end

        def unstarted?
          worker.nil? || forked?
        end

        def running?
          worker && worker.alive?
        end

        def error?
          !@error.nil?
        end

        def finished?
          worker && worker.status == false
        end

        def forked?
          !pid.nil? && pid != Process.pid
        end

        def fork_policy
          @fork_policy ||= FORK_POLICY_STOP
        end

        def after_fork
          # Do nothing by default
        end

        protected

        attr_writer \
          :result

        def mutex
          @mutex ||= Mutex.new
        end

        private

        attr_reader \
          :pid

        def mutex_after_fork
          @mutex_after_fork ||= Mutex.new
        end

        def worker
          @worker ||= nil
        end

        def start(&block)
          mutex.synchronize do
            return if running?
            if forked?
              case fork_policy
              when FORK_POLICY_STOP
                stop_fork
              when FORK_POLICY_RESTART
                restart_after_fork(&block)
              end
            elsif !run?
              start_worker(&block)
            end
          end
        end

        def start_worker
          @run = true
          @pid = Process.pid
          @error = nil
          Logger.log.debug("Starting thread in the process: #{Process.pid}")

          @worker = ::Thread.new do
            begin
              yield
            rescue StandardError => e
              @error = e
              Logger.log.debug("Worker thread error. Cause #{e.message} Location: #{e.backtrace.first}")
            end
          end
        end

        def stop_fork
          mutex_after_fork.synchronize do
            if forked?
              # Trigger callback to allow workers to reset themselves accordingly
              after_fork

              # Reset and turn off
              @pid = Process.pid
              @run = false
            end
          end
        end

        def restart_after_fork(&block)
          mutex_after_fork.synchronize do
            if forked?
              # Trigger callback to allow workers to reset themselves accordingly
              after_fork

              # Start worker
              start_worker(&block)
            end
          end
        end
      end
    end
  end
end
