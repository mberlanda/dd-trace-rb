require 'spec_helper'

require 'ddtrace'
require 'ddtrace/workers/runtime_metrics'

RSpec.describe Datadog::Workers::RuntimeMetrics do
  subject(:worker) { described_class.new(metrics, options) }
  let(:metrics) { instance_double(Datadog::Runtime::Metrics) }
  let(:options) { {} }

  describe '#perform' do
    subject(:perform) { worker.perform }
    after { worker.stop }

    it 'starts a worker thread' do
      perform
      expect(worker).to have_attributes(
        metrics: metrics,
        run?: true,
        running?: true,
        unstarted?: false,
        forked?: false,
        fork_policy: :stop,
        result: nil
      )
    end
  end

  describe 'integration tests' do
    let(:options) { { fork_policy: fork_policy } }

    before do
      allow(Datadog.configuration).to receive(:runtime_metrics_enabled)
        .and_return(true)
    end

    describe 'forking' do
      context 'when the process forks' do
        before { allow(metrics).to receive(:flush) }
        after { worker.stop }

        context 'with :stop fork policy' do
          let(:fork_policy) { :sync }

          it 'does not produce metrics' do
            # Start worker in main process
            worker.perform

            expect_in_fork do
              # Capture the flush
              @flushed = false
              allow(metrics).to receive(:flush) do
                @flushed = true
              end

              # Restart worker & wait
              worker.perform
              try_wait_until { @flushed }

              # Verify state of the worker
              expect(worker.error?).to be false
              expect(metrics).to_not have_received(:flush)
            end
          end
        end

        context 'with :restart fork policy' do
          let(:fork_policy) { :restart }

          it 'continues producing metrics' do
            # Start worker
            worker.perform

            expect_in_fork do
              # Capture the flush
              @flushed = false
              allow(metrics).to receive(:flush) do
                @flushed = true
              end

              # Restart worker & wait
              worker.perform
              try_wait_until { @flushed }

              # Verify state of the worker
              expect(worker.error?).to be false
              expect(metrics).to have_received(:flush).at_least(:once)
            end
          end
        end
      end
    end
  end
end
