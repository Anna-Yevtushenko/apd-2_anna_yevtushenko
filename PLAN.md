# Implementation Plan

Complete this before implementation and update it when the plan changes.

## Approach

1) First - familiarize with the principles of Jobs, Task and asynchronous execution in Swift. Review the existing project code and analyze the initial tests to see which scenarios are already covered.
2) Next - implement `TaskProcessor` as a Swift actor to safely manage asynchronous tasks, addressing the requirements step-by-step:
- Validate `maximumConcurrency` and the uniqueness of all Job IDs before execution begins. If the data is invalid then return an appropriate error without starting any workers
- Run no more than `maximumConcurrency` tasks simultaneously
- Store results based on the original job indices so they can be returned in the correct order, regardless of completion order
- Handle three possible outcomes: `succeeded`, `failed`, `cancelled`

3) Add custom tests to verify positive, negative,edge-case scenarios, run `swift test`
4) Once the main implementation is complete, proceed to the bonus task: add a `RetryPolicy` that allows retrying failed attempts up to a set limit, without exceeding `maximumConcurrency`

## Milestones

- [x] Validate configuration and all job IDs before starting work.
- [x] Schedule jobs within the concurrency limit and preserve input order.
- [x] Map success, ordinary errors, and cancellation outcomes.
- [x] Handle caller cancellation and never-started jobs.
- [x] Add at least six distinct student-authored tests and run `swift test`.

## Risks and checks

RISKS: 
1) Concurrency and result ordering - verify adherence to maximumConcurrency and the preservation of the original job order, regardless of completion timing
2) Validation and error handling - verify handling of duplicate IDs, worker errors and correct processing of CancellationError
3) Cancellation and retries - verify the cancellation of active jobs, ensure no new jobs are started and confirm the correct operation of RetryPolicy without exceeding the concurrency limit

CHECKS: swift test launched

Test Suite 'All tests' passed at 2026-10-11 12:01:33.380.
Executed 21 tests, with 0 failures (0 unexpected) in 0.012 (0.030) seconds
Program ended with exit code: 0
