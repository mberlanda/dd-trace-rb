require 'ddtrace/worker'
require 'ddtrace/workers/async'
require 'ddtrace/workers/loop'

module Datadog
  module Workers
    # Emits runtime metrics asynchronously on a timed loop
    class RuntimeMetrics < Worker
      include Workers::IntervalLoop
      include Workers::Async::Thread

      attr_reader \
        :metrics

      def initialize(metrics, options = {})
        @metrics = metrics

        # Workers::Async::Thread settings
        self.fork_policy = options.fetch(:fork_policy, Workers::Async::Thread::FORK_POLICY_STOP)

        # Workers::IntervalLoop settings
        self.interval = options[:interval] if options.key?(:interval)
        self.back_off_ratio = options[:back_off_ratio] if options.key?(:back_off_ratio)
        self.back_off_max = options[:back_off_max] if options.key?(:back_off_max)
      end

      def perform
        return unless Datadog.configuration.runtime_metrics_enabled && metrics
        metrics.flush
        true
      end
    end
  end
end
