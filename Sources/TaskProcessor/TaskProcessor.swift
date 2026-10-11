public struct Job: Sendable, Equatable {
    public let id: String
    public let input: Int
    
    public init(id: String, input: Int) {
        self.id = id
        self.input = input
    }
}

public enum JobOutcome: Sendable, Equatable {
    case succeeded(Int)
    case failed
    case cancelled
}

public struct JobResult: Sendable, Equatable {
    public let id: String
    public let outcome: JobOutcome
    
    public init(id: String, outcome: JobOutcome) {
        self.id = id
        self.outcome = outcome
    }
}

public enum TaskProcessorError: Error, Equatable {
    case nonPositiveMaximumConcurrency
    case duplicateID(String)
}


public struct RetryPolicy: Sendable, Equatable {
    public let maximumAttempts: Int
    
    public init(maximumAttempts: Int) throws {
        guard maximumAttempts > 0 else {
            throw RetryPolicyError.nonPositiveMaximumAttempts
        }
        
        self.maximumAttempts = maximumAttempts
    }
}

public enum RetryPolicyError: Error, Equatable {
    case nonPositiveMaximumAttempts
}

public actor TaskProcessor {
    private let maximumConcurrency: Int
    
    private func addJob(
        at index: Int,
        jobs: [Job],
        worker: @escaping @Sendable (Job) async throws -> Int,
        to group: inout TaskGroup<(Int, JobResult)>
    ) {
        let job = jobs[index]
        
        group.addTask {
            let outcome: JobOutcome
            
            do {
                let value = try await worker(job)
                outcome = .succeeded(value)
            } catch is CancellationError {
                outcome = .cancelled
            } catch {
                outcome = .failed
            }
            
            return (
                index,
                JobResult(id: job.id, outcome: outcome)
            )
        }
    }
    
    private func validateJobs(_ jobs: [Job]) throws {
        var seenIDs = Set<String>()
        
        for job in jobs {
            guard seenIDs.insert(job.id).inserted else {
                throw TaskProcessorError.duplicateID(job.id)
            }
        }
    }
    
    private func executeJobs(
        _ jobs: [Job],
        worker: @escaping @Sendable (Job) async throws -> Int
    ) async -> [JobResult] {
        await withTaskGroup(of: (Int, JobResult).self) { group in
            let initialCount = min(maximumConcurrency, jobs.count)
            var nextIndex = 0
            var cancellationObserved = false
            
            var results = Array<JobResult?>(
                repeating: nil,
                count: jobs.count
            )
            
            for _ in 0..<initialCount {
                if Task.isCancelled {
                    cancellationObserved = true
                    group.cancelAll()
                    break
                }
                
                addJob(at: nextIndex, jobs: jobs, worker: worker, to: &group)
                nextIndex += 1
            }
            
            while let (index, result) = await group.next() {
                results[index] = result
                
                if Task.isCancelled {
                    cancellationObserved = true
                    group.cancelAll()
                }
                
                if !cancellationObserved && nextIndex < jobs.count {
                    addJob(at: nextIndex, jobs: jobs, worker: worker, to: &group)
                    nextIndex += 1
                }
            }
            
            if cancellationObserved {
                for index in nextIndex..<jobs.count {
                    results[index] = JobResult(
                        id: jobs[index].id,
                        outcome: .cancelled
                    )
                }
            }
            
            return results.compactMap { $0 }
        }
    }
    
    
    public init(maximumConcurrency: Int) throws {
        guard maximumConcurrency > 0 else {
            throw TaskProcessorError.nonPositiveMaximumConcurrency
        }
        
        self.maximumConcurrency = maximumConcurrency
    }
    
    public func process(
        _ jobs: [Job],
        worker: @escaping @Sendable (Job) async throws -> Int
    ) async throws -> [JobResult] {
        try validateJobs(jobs)
        
        guard !jobs.isEmpty else {
            return []
        }
        
        return await executeJobs(jobs, worker: worker)
    }
}

public extension TaskProcessor {
    func process(
        _ jobs: [Job],
        retryPolicy: RetryPolicy,
        worker: @escaping @Sendable (Job, Int) async throws -> Int
    ) async throws -> [JobResult] {
        try await process(jobs) { job in
            for attempt in 1...retryPolicy.maximumAttempts {
                try Task.checkCancellation()
                
                do {
                    return try await worker(job, attempt)
                } catch is CancellationError {
                    throw CancellationError()
                } catch {
                    if attempt == retryPolicy.maximumAttempts {
                        throw error
                    }
                }
            }
            
            throw CancellationError()
        }
    }
}
