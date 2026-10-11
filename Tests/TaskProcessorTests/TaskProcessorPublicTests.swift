import XCTest
@testable import TaskProcessor

private enum SampleWorkerError: Error {
    case expected
    case workerShouldNotRun
}

private actor Gate {
    private var releasedIDs: Set<String> = []
    private var waiters: [String: CheckedContinuation<Void, Never>] = [:]
    private var waitingObservers: [String: [CheckedContinuation<Void, Never>]] = [:]
    private var passedIDs: Set<String> = []
    private var passedObservers: [String: [CheckedContinuation<Void, Never>]] = [:]
    
    func wait(for id: String) async {
        if releasedIDs.remove(id) != nil {
            markPassed(id)
            return
        }
        
        await withCheckedContinuation { continuation in
            waiters[id] = continuation
            let observers = waitingObservers.removeValue(forKey: id) ?? []
            for observer in observers {
                observer.resume()
            }
        }
        markPassed(id)
    }
    
    func waitUntilWaiting(for id: String) async {
        if waiters[id] != nil {
            return
        }
        
        await withCheckedContinuation { continuation in
            waitingObservers[id, default: []].append(continuation)
        }
    }
    
    func waitUntilPassed(_ id: String) async {
        if passedIDs.contains(id) {
            return
        }
        
        await withCheckedContinuation { continuation in
            passedObservers[id, default: []].append(continuation)
        }
    }
    
    func release(_ id: String) {
        if let waiter = waiters.removeValue(forKey: id) {
            waiter.resume()
        } else {
            releasedIDs.insert(id)
        }
    }
    
    private func markPassed(_ id: String) {
        passedIDs.insert(id)
        let observers = passedObservers.removeValue(forKey: id) ?? []
        for observer in observers {
            observer.resume()
        }
    }
}

private actor WorkerTracker {
    private var startedJobs: [String] = []
    private var activeWorkers = 0
    private var maximumActiveWorkers = 0
    
    func start(_ id: String) {
        startedJobs.append(id)
        activeWorkers += 1
        maximumActiveWorkers = max(
            maximumActiveWorkers,
            activeWorkers
        )
    }
    
    func finish() {
        activeWorkers -= 1
    }
    
    func started() -> [String] {
        startedJobs
    }
    
    func peakConcurrency() -> Int {
        maximumActiveWorkers
    }
}

private actor AttemptTracker {
    private var attempts: [Int] = []
    
    func record(_ attempt: Int) {
        attempts.append(attempt)
    }
    
    func recordedAttempts() -> [Int] {
        attempts
    }
}

@MainActor
final class TaskProcessorPublicTests: XCTestCase {
    func testOneSuccessfulJob() async throws {
        let processor = try TaskProcessor(maximumConcurrency: 1)
        let job = Job(id: "one", input: 21)
        
        let results = try await processor.process([job]) { job in
            job.input * 2
        }
        
        XCTAssertEqual(
            results,
            [JobResult(id: "one", outcome: .succeeded(42))]
        )
    }
    
    func testEmptyJobListReturnsEmptyWithoutCallingWorker() async throws {
        let processor = try TaskProcessor(maximumConcurrency: 2)
        
        let results = try await processor.process([]) { _ in
            throw SampleWorkerError.workerShouldNotRun
        }
        
        XCTAssertEqual(results, [])
    }
    
    func testOrdinaryWorkerErrorBecomesFailedOutcome() async throws {
        let processor = try TaskProcessor(maximumConcurrency: 1)
        let job = Job(id: "fails", input: 7)
        
        let results = try await processor.process([job]) { _ in
            throw SampleWorkerError.expected
        }
        
        XCTAssertEqual(
            results,
            [JobResult(id: "fails", outcome: .failed)]
        )
    }
    
    func testResultsKeepInputOrderWhenSecondJobPassesGateFirst() async throws {
        let processor = try TaskProcessor(maximumConcurrency: 2)
        let gate = Gate()
        let jobs = [
            Job(id: "first", input: 1),
            Job(id: "second", input: 2)
        ]
        
        let processing = Task {
            try await processor.process(jobs) { job in
                await gate.wait(for: job.id)
                return job.input * 10
            }
        }
        
        await gate.waitUntilWaiting(for: "first")
        await gate.waitUntilWaiting(for: "second")
        await gate.release("second")
        await gate.waitUntilPassed("second")
        await gate.release("first")
        
        let results = try await processing.value
        XCTAssertEqual(
            results,
            [
                JobResult(id: "first", outcome: .succeeded(10)),
                JobResult(id: "second", outcome: .succeeded(20))
            ]
        )
    }
    
    func testZeroMaximumConcurrencyIsRejected() {
        XCTAssertThrowsError(try TaskProcessor(maximumConcurrency: 0)) { error in
            XCTAssertEqual(
                error as? TaskProcessorError,
                .nonPositiveMaximumConcurrency
            )
        }
    }
}

@MainActor
final class TaskProcessorPositiveTests: XCTestCase {
    
    func testMultipleSuccessfulJobs() async throws {
        let processor = try TaskProcessor(maximumConcurrency: 3)
        let jobs = [
            Job(id: "A", input: 15),
            Job(id: "B", input: 30),
            Job(id: "C", input: -6)
        ]
        
        let results = try await processor.process(jobs) { job in
            job.input * 2
        }
        
        XCTAssertEqual(results, [
            JobResult(id: "A", outcome: .succeeded(30)),
            JobResult(id: "B", outcome: .succeeded(60)),
            JobResult(id: "C", outcome: .succeeded(-12))
        ])
    }
    
    func testMaximumConcurrencyOneExecutesSequentially() async throws {
        let processor = try TaskProcessor(maximumConcurrency: 1)
        let gate = Gate()
        let tracker = WorkerTracker()
        
        let jobs = [
            Job(id: "A", input: 1),
            Job(id: "B", input: 2)
        ]
        
        let processing = Task {
            try await processor.process(jobs) { job in
                await tracker.start(job.id)
                await gate.wait(for: job.id)
                await tracker.finish()
                return job.input
            }
        }
        
        await gate.waitUntilWaiting(for: "A")
        
        let initiallyStarted = await tracker.started()
        XCTAssertEqual(initiallyStarted, ["A"])
        
        await gate.release("A")
        await gate.waitUntilWaiting(for: "B")
        
        let startedAfterRelease = await tracker.started()
        XCTAssertEqual(startedAfterRelease, ["A", "B"])
        
        await gate.release("B")
        
        let results = try await processing.value
        let peak = await tracker.peakConcurrency()
        
        XCTAssertEqual(peak, 1)
        XCTAssertEqual(results.count, 2)
    }
    
    func testMaximumConcurrencyTwoLimitsWorkers() async throws {
        let processor = try TaskProcessor(maximumConcurrency: 2)
        let gate = Gate()
        let tracker = WorkerTracker()
        
        let jobs = [
            Job(id: "A", input: 1),
            Job(id: "B", input: 2),
            Job(id: "C", input: 3)
        ]
        
        let processing = Task {
            try await processor.process(jobs) { job in
                await tracker.start(job.id)
                await gate.wait(for: job.id)
                await tracker.finish()
                return job.input
            }
        }
        
        await gate.waitUntilWaiting(for: "A")
        await gate.waitUntilWaiting(for: "B")
        
        let initiallyStarted = await tracker.started()
        XCTAssertEqual(Set(initiallyStarted), Set(["A", "B"]))
        
        await gate.release("A")
        await gate.waitUntilWaiting(for: "C")
        
        await gate.release("B")
        await gate.release("C")
        
        let results = try await processing.value
        let peak = await tracker.peakConcurrency()
        
        XCTAssertEqual(peak, 2)
        XCTAssertEqual(results.count, 3)
        
        
        XCTAssertEqual(results, [
            JobResult(id: "A", outcome: .succeeded(1)),
            JobResult(id: "B", outcome: .succeeded(2)),
            JobResult(id: "C", outcome: .succeeded(3))
        ])
    }
    
    func testFewerJobsThanConcurrencyLimit() async throws {
        let processor = try TaskProcessor(maximumConcurrency: 5)
        let gate = Gate()
        let tracker = WorkerTracker()
        
        let jobs = [
            Job(id: "A", input: 10),
            Job(id: "B", input: 20)
        ]
        
        let processing = Task {
            try await processor.process(jobs) { job in
                await tracker.start(job.id)
                await gate.wait(for: job.id)
                await tracker.finish()
                return job.input
            }
        }
        
        await gate.waitUntilWaiting(for: "A")
        await gate.waitUntilWaiting(for: "B")
        
        let peak = await tracker.peakConcurrency()
        XCTAssertEqual(peak, 2)
        
        await gate.release("A")
        await gate.release("B")
        
        let results = try await processing.value
        XCTAssertEqual(results.count, 2)
    }
    
    func testProcessingContinuesAfterWorkerFailure() async throws {
        let processor = try TaskProcessor(maximumConcurrency: 1)
        
        let jobs = [
            Job(id: "A", input: 10),
            Job(id: "B", input: 20)
        ]
        
        let results = try await processor.process(jobs) { job in
            if job.id == "A" {
                throw SampleWorkerError.expected
            }
            
            return job.input * 2
        }
        
        XCTAssertEqual(results, [
            JobResult(id: "A", outcome: .failed),
            JobResult(id: "B", outcome: .succeeded(40))
        ])
    }
}

@MainActor
final class TaskProcessorNegativeTests: XCTestCase {
    
    func testNegativeMaximumConcurrencyIsRejected() {
        XCTAssertThrowsError(
            try TaskProcessor(maximumConcurrency: -1)
        ) { error in
            XCTAssertEqual(
                error as? TaskProcessorError,
                .nonPositiveMaximumConcurrency
            )
        }
    }
    
    func testDuplicateJobIDIsRejected() async throws {
        let processor = try TaskProcessor(maximumConcurrency: 2)
        
        let jobs = [
            Job(id: "A", input: 1),
            Job(id: "B", input: 2),
            Job(id: "A", input: 3)
        ]
        
        do {
            _ = try await processor.process(jobs) { job in
                job.input
            }
            XCTFail("Expected duplicateID error")
        } catch {
            XCTAssertEqual(
                error as? TaskProcessorError,
                .duplicateID("A")
            )
        }
    }
    
    func testFirstRepeatedIDIsReported() async throws {
        let processor = try TaskProcessor(maximumConcurrency: 2)
        
        let jobs = [
            Job(id: "A", input: 1),
            Job(id: "B", input: 2),
            Job(id: "B", input: 3),
            Job(id: "A", input: 4)
        ]
        
        do {
            _ = try await processor.process(jobs) { job in
                job.input
            }
            XCTFail("Expected duplicateID error")
        } catch {
            XCTAssertEqual(
                error as? TaskProcessorError,
                .duplicateID("B")
            )
        }
    }
    
    func testDuplicateValidationHappensBeforeWorkerStarts() async throws {
        let processor = try TaskProcessor(maximumConcurrency: 2)
        let tracker = WorkerTracker()
        
        let jobs = [
            Job(id: "A", input: 1),
            Job(id: "B", input: 2),
            Job(id: "A", input: 3)
        ]
        
        do {
            _ = try await processor.process(jobs) { job in
                await tracker.start(job.id)
                return job.input
            }
            XCTFail("Expected duplicateID error")
        } catch {
            XCTAssertEqual(
                error as? TaskProcessorError,
                .duplicateID("A")
            )
        }
        
        let started = await tracker.started()
        XCTAssertTrue(started.isEmpty)
    }
    
    func testWorkerCancellationDoesNotStopOtherJobs() async throws {
        let processor = try TaskProcessor(maximumConcurrency: 1)
        
        let jobs = [
            Job(id: "A", input: 1),
            Job(id: "B", input: 2)
        ]
        
        let results = try await processor.process(jobs) { job in
            if job.id == "A" {
                throw CancellationError()
            }
            
            return job.input * 10
        }
        
        XCTAssertEqual(results, [
            JobResult(id: "A", outcome: .cancelled),
            JobResult(id: "B", outcome: .succeeded(20))
        ])
    }
    
    func testMixedWorkerOutcomesKeepInputOrder() async throws {
        let processor = try TaskProcessor(maximumConcurrency: 3)
        
        let jobs = [
            Job(id: "A", input: 1),
            Job(id: "B", input: 2),
            Job(id: "C", input: 3)
        ]
        
        let results = try await processor.process(jobs) { job in
            switch job.id {
            case "A":
                return 10
            case "B":
                throw SampleWorkerError.expected
            default:
                throw CancellationError()
            }
        }
        
        XCTAssertEqual(results, [
            JobResult(id: "A", outcome: .succeeded(10)),
            JobResult(id: "B", outcome: .failed),
            JobResult(id: "C", outcome: .cancelled)
        ])
    }
    
    func testCallerCancellationStopsSchedulingNewJobs() async throws {
        let processor = try TaskProcessor(maximumConcurrency: 1)
        let tracker = WorkerTracker()
        let gate = Gate()

        let jobs = [
            Job(id: "A", input: 1),
            Job(id: "B", input: 2),
            Job(id: "C", input: 3)
        ]

        let processing = Task {
            try await processor.process(jobs) { job in
                await tracker.start(job.id)
                await gate.wait(for: job.id)
                try Task.checkCancellation()
                return job.input
            }
        }

        await gate.waitUntilWaiting(for: "A")
        processing.cancel()
        await gate.release("A")

        let results = try await processing.value
        let started = await tracker.started()

        XCTAssertEqual(started, ["A"])

        XCTAssertEqual(results, [
            JobResult(id: "A", outcome: .cancelled),
            JobResult(id: "B", outcome: .cancelled),
            JobResult(id: "C", outcome: .cancelled)
        ])
    }}

@MainActor
final class TaskProcessorRetryTests: XCTestCase {
    
    func testInvalidMaximumAttemptsIsRejected() {
        for attempts in [0, -1] {
            XCTAssertThrowsError(
                try RetryPolicy(maximumAttempts: attempts)
            ) { error in
                XCTAssertEqual(
                    error as? RetryPolicyError,
                    .nonPositiveMaximumAttempts
                )
            }
        }
    }
    
    func testRetrySucceedsOnFirstAttempt() async throws {
        let processor = try TaskProcessor(maximumConcurrency: 1)
        let policy = try RetryPolicy(maximumAttempts: 3)
        let tracker = AttemptTracker()
        
        let jobs = [Job(id: "A", input: 10)]
        
        let results = try await processor.process(
            jobs,
            retryPolicy: policy
        ) { job, attempt in
            await tracker.record(attempt)
            return job.input * 2
        }
        
        let attempts = await tracker.recordedAttempts()
        
        XCTAssertEqual(attempts, [1])
        XCTAssertEqual(results, [
            JobResult(id: "A", outcome: .succeeded(20))
        ])
    }
    
    func testRetrySucceedsAfterFailure() async throws {
        let processor = try TaskProcessor(maximumConcurrency: 1)
        let policy = try RetryPolicy(maximumAttempts: 3)
        let tracker = AttemptTracker()
        
        let jobs = [Job(id: "A", input: 10)]
        
        let results = try await processor.process(
            jobs,
            retryPolicy: policy
        ) { job, attempt in
            await tracker.record(attempt)
            
            if attempt == 1 {
                throw SampleWorkerError.expected
            }
            
            return job.input * 2
        }
        
        let attempts = await tracker.recordedAttempts()
        
        XCTAssertEqual(attempts, [1, 2])
        XCTAssertEqual(results, [
            JobResult(id: "A", outcome: .succeeded(20))
        ])
    }
    
    func testRetryFailsAfterMaximumAttempts() async throws {
        let processor = try TaskProcessor(maximumConcurrency: 1)
        let policy = try RetryPolicy(maximumAttempts: 3)
        let tracker = AttemptTracker()
        
        let jobs = [Job(id: "A", input: 10)]
        
        let results = try await processor.process(
            jobs,
            retryPolicy: policy
        ) { _, attempt in
            await tracker.record(attempt)
            throw SampleWorkerError.expected
        }
        
        let attempts = await tracker.recordedAttempts()
        
        XCTAssertEqual(attempts, [1, 2, 3])
        XCTAssertEqual(results, [
            JobResult(id: "A", outcome: .failed)
        ])
    }
}
