require 'spec_helper'

require 'ddtrace'
require 'ddtrace/workers/trace_writer'

RSpec.describe Datadog::Workers::TraceWriter do
  describe '#write' do
    # TODO
  end

  describe '#perform' do
    # TODO
  end

  describe '#flush_traces' do
    # TODO
  end

  describe '#process_traces' do
    # TODO
  end

  describe '#inject_hostname!' do
    # TODO
  end

  describe '#flush_completed' do
    # TODO
  end

  describe described_class::FlushCompleted do
    describe '#name' do
      # TODO
    end

    describe '#publish' do
      # TODO
    end
  end
end

RSpec.describe Datadog::Workers::AsyncTraceWriter do
  subject(:writer) { described_class.new(options) }
  let(:options) { {} }

  describe '#perform' do
    subject(:perform) { writer.perform }
    after { writer.stop }

    it 'starts a worker thread' do
      is_expected.to be_a_kind_of(Thread)
      expect(writer).to have_attributes(
        run?: true,
        running?: true,
        unstarted?: false,
        forked?: false,
        fork_policy: :restart,
        result: nil
      )
    end
  end

  describe '#write' do
    subject(:write) { writer.write(trace) }
    let(:trace) { double('trace') }

    context 'when in async mode' do
      before { allow(writer).to receive(:async?).and_return true }

      context 'and given a trace' do
        before do
          allow(writer.buffer).to receive(:push)
          write
        end

        it { expect(writer.buffer).to have_received(:push).with(trace) }
      end
    end

    context 'when not in async mode' do
      before { allow(writer).to receive(:async?).and_return false }

      context 'and given a trace' do
        before do
          allow(writer).to receive(:write_traces)
          write
        end

        it { expect(writer).to have_received(:write_traces).with([trace]) }
      end
    end
  end

  describe '#enqueue' do
    subject(:enqueue) { writer.enqueue(trace) }
    let(:trace) { double('trace') }

    before do
      allow(writer.buffer).to receive(:push)
      enqueue
    end

    it { expect(writer.buffer).to have_received(:push).with(trace) }
  end

  describe '#dequeue' do
    subject(:dequeue) { writer.dequeue }
    let(:traces) { double('traces') }

    before do
      allow(writer.buffer).to receive(:pop)
        .and_return(traces)
    end

    it { is_expected.to eq([traces]) }
  end

  describe '#stop' do
    subject(:stop) { writer.stop }

    shared_context 'shuts down the worker' do
      before do
        expect(writer.buffer).to receive(:close)
        expect(writer).to receive(:join)
          .with(described_class::SHUTDOWN_TIMEOUT)
      end
    end

    context 'when the worker has not been started' do
      include_context 'shuts down the worker'
      it { is_expected.to be nil }
    end

    context 'when the worker has been started' do
      include_context 'shuts down the worker'
      before { writer.perform }
      it { is_expected.to be true }
    end

    context 'called multiple times' do
      before { writer.perform }

      it do
        expect(writer.stop).to be true
        expect(writer.stop).to be nil
      end
    end
  end

  describe '#async=' do
    subject(:set_async) { writer.async = value }

    context 'given true' do
      let(:value) { true }

      it do
        is_expected.to be true
        expect(writer.async?).to be true
      end
    end

    context 'given false' do
      let(:value) { false }

      it do
        is_expected.to be false
        expect(writer.async?).to be false
      end
    end
  end

  describe '#async?' do
    subject(:async?) { writer.async? }

    context 'by default' do
      it { is_expected.to be true }
    end

    context 'when set to false' do
      before { writer.async = false }

      it do
        is_expected.to be false
      end
    end

    context 'when set to truthy' do
      before { writer.async = 1 }

      it do
        is_expected.to be false
      end
    end
  end

  describe '#fork_policy=' do
    subject(:set_fork_policy) { writer.fork_policy = value }

    context 'given FORK_POLICY_ASYNC' do
      let(:value) { described_class::FORK_POLICY_ASYNC }

      it do
        is_expected.to be value
        expect(writer.fork_policy).to eq(Datadog::Workers::Async::Thread::FORK_POLICY_RESTART)
      end
    end

    context 'given FORK_POLICY_SYNC' do
      let(:value) { described_class::FORK_POLICY_SYNC }

      it do
        is_expected.to be value
        expect(writer.fork_policy).to eq(Datadog::Workers::Async::Thread::FORK_POLICY_STOP)
      end
    end
  end

  describe '#after_fork' do
    subject(:after_fork) { writer.after_fork }

    it { expect { after_fork }.to(change { writer.buffer }) }

    context 'when fork_policy is' do
      before { writer.fork_policy = fork_policy }

      context 'FORK_POLICY_ASYNC' do
        let(:fork_policy) { described_class::FORK_POLICY_ASYNC }

        it do
          expect { after_fork }.to_not(change { writer.async? })
        end
      end

      context 'FORK_POLICY_SYNC' do
        let(:fork_policy) { described_class::FORK_POLICY_SYNC }

        it do
          expect { after_fork }.to change { writer.async? }.from(true).to(false)
        end
      end
    end
  end

  describe 'integration tests' do
    let(:options) { { transport: transport, fork_policy: fork_policy } }
    let(:transport) { Datadog::Transport::HTTP.default { |t| t.adapter :test, output } }
    let(:output) { [] }

    describe 'forking' do
      context 'when the process forks and a trace is written' do
        let(:traces) { get_test_traces(2) }

        before do
          allow(writer).to receive(:after_fork)
            .and_call_original
          allow(writer.transport).to receive(:send_traces)
            .and_call_original
        end

        after { writer.stop }

        context 'with :sync fork policy' do
          let(:fork_policy) { :sync }

          it 'does not drop any traces' do
            # Start writer in main process
            writer.perform

            expect_in_fork do
              traces.each do |trace|
                expect(writer.write(trace)).to be_a_kind_of(Datadog::Transport::HTTP::Response)
              end

              expect(writer).to have_received(:after_fork).once

              traces.each do |trace|
                expect(writer.transport).to have_received(:send_traces)
                  .with([trace])
              end

              expect(writer.buffer).to be_empty
            end
          end
        end

        context 'with :async fork policy' do
          let(:fork_policy) { :async }
          let(:flushed_traces) { [] }

          it 'does not drop any traces' do
            # Start writer in main process
            writer.perform

            expect_in_fork do
              # Queue up traces, wait for worker to process them.
              traces.each { |trace| writer.write(trace) }
              try_wait_until(attempts: 30) { !writer.work_pending? }

              # Verify state of the writer
              expect(writer).to have_received(:after_fork).once
              expect(writer.buffer).to be_empty
              expect(writer.error?).to be false

              expect(writer.transport).to have_received(:send_traces).at_most(2).times do |traces|
                flushed_traces.concat(traces)
              end

              expect(flushed_traces).to_not be_empty
              expect(flushed_traces).to have(2).items
              expect(flushed_traces).to include(*traces)
            end
          end
        end
      end
    end
  end
end
