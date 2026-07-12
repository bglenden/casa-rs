import XCTest
@testable import CasarsMacCore

final class TutorialNotebookPrototypeTests: XCTestCase {
    func testFactoryBuildsEveryScenarioWithoutAutomaticAcquisition() throws {
        XCTAssertEqual(
            TutorialNotebookPrototypeScenario.allCases.map(\.rawValue),
            ["happy-path", "checksum-failure", "disk-failure", "offline", "unsafe-archive"]
        )

        for scenario in TutorialNotebookPrototypeScenario.allCases {
            let projection = TutorialNotebookPrototypeFixtureAdapter.make(scenario: scenario)

            XCTAssertEqual(projection.prototypeKind, .tutorial)
            XCTAssertEqual(projection.scenario, scenario)
            XCTAssertEqual(projection.dataset.phase, .missing)
            XCTAssertFalse(projection.dataset.isStaged)
            XCTAssertFalse(projection.dataset.isReady)
            XCTAssertEqual(projection.dataset.currentGeneration, 0)
            XCTAssertTrue(projection.dataset.attempts.isEmpty)
            XCTAssertNil(projection.activeApproval)
            XCTAssertEqual(projection.learnerNotebook.receipts, [projection.fixtureTask])
            XCTAssertTrue(projection.fixtureTask.revisions.isEmpty)
        }
    }

    func testFactoryCarriesExactTWHyaApprovalFactsInMemory() {
        let projection = TutorialNotebookPrototypeFixtureAdapter.make(scenario: .happyPath)
        let facts = projection.approvalFacts

        XCTAssertEqual(facts.scheme, "https")
        XCTAssertEqual(facts.requestedURL, facts.resolvedURL)
        XCTAssertTrue(facts.redirects.isEmpty)
        XCTAssertEqual(facts.expectedSizeBytes, 435_742_720)
        XCTAssertEqual(
            facts.expectedSHA256,
            "f0cfeee5b9dec09ac9ed4d3e4e048d5eb28023c11cbc8295c09ddefe6b8a97b2"
        )
        XCTAssertEqual(facts.destination, "data/twhya_calibrated.ms")
        XCTAssertEqual(facts.requiredDiskBytes, 1_307_228_160)
        XCTAssertTrue(facts.hasEnoughDisk)
        XCTAssertEqual(facts.optionalChecks.count, 2)
        XCTAssertTrue(facts.optionalChecks.allSatisfy(\.isEnabled))
    }

    func testApprovalMustBeShownAndCanBeDismissedWithoutStarting() throws {
        var projection = TutorialNotebookPrototypeFixtureAdapter.make(scenario: .happyPath)

        XCTAssertNil(projection.approve())
        XCTAssertTrue(projection.showApproval())
        XCTAssertEqual(projection.dataset.phase, .approvalRequired)
        XCTAssertEqual(projection.activeApproval, projection.approvalFacts)
        XCTAssertTrue(projection.dataset.attempts.isEmpty)
        XCTAssertTrue(projection.dismissApproval())
        XCTAssertEqual(projection.dataset.phase, .missing)
        XCTAssertNil(projection.activeApproval)
        XCTAssertTrue(projection.dataset.attempts.isEmpty)

        XCTAssertTrue(projection.showApproval())
        let generation = try XCTUnwrap(projection.approve())
        XCTAssertEqual(generation, 1)
        XCTAssertEqual(projection.dataset.phase, .downloading)
        XCTAssertNil(projection.activeApproval)
        XCTAssertFalse(projection.dataset.isStaged)
    }

    func testHappyPathUsesLegalDownloadVerifyUnpackReadyFlow() throws {
        var projection = TutorialNotebookPrototypeFixtureAdapter.make(scenario: .happyPath)
        let generation = try beginApprovedAttempt(&projection)

        XCTAssertTrue(projection.advance(generation: generation))
        XCTAssertEqual(projection.dataset.phase, .downloading)
        XCTAssertEqual(projection.dataset.currentAttempt?.progress, 0.5)
        XCTAssertTrue(projection.advance(generation: generation))
        XCTAssertEqual(projection.dataset.phase, .verifying)
        XCTAssertTrue(projection.advance(generation: generation))
        XCTAssertEqual(projection.dataset.phase, .unpacking)
        XCTAssertTrue(projection.advance(generation: generation))
        XCTAssertEqual(projection.dataset.phase, .ready)
        XCTAssertTrue(projection.dataset.isStaged)
        XCTAssertTrue(projection.dataset.isReady)
        XCTAssertFalse(projection.advance(generation: generation))
    }

    func testCancellationResumesFromPartialBytesInANewGeneration() throws {
        var projection = TutorialNotebookPrototypeFixtureAdapter.make(scenario: .happyPath)
        let firstGeneration = try beginApprovedAttempt(&projection)
        XCTAssertTrue(projection.advance(generation: firstGeneration))
        let partialBytes = try XCTUnwrap(projection.dataset.currentAttempt?.downloadedBytes)

        XCTAssertTrue(projection.cancel())
        XCTAssertEqual(projection.dataset.phase, .cancelled)
        XCTAssertFalse(projection.dataset.isStaged)
        XCTAssertFalse(projection.dataset.isReady)
        XCTAssertFalse(projection.advance(generation: firstGeneration))

        let resumedGeneration = try XCTUnwrap(projection.resume())
        XCTAssertGreaterThan(resumedGeneration, firstGeneration)
        XCTAssertEqual(projection.dataset.currentAttempt?.kind, .resume)
        XCTAssertEqual(projection.dataset.currentAttempt?.resumeOffsetBytes, partialBytes)
        XCTAssertEqual(projection.dataset.currentAttempt?.downloadedBytes, partialBytes)
        try advanceUntilReady(&projection, generation: resumedGeneration)
    }

    func testRestartRejectsStaleGenerationCompletion() throws {
        var projection = TutorialNotebookPrototypeFixtureAdapter.make(scenario: .happyPath)
        let firstGeneration = try beginApprovedAttempt(&projection)
        XCTAssertTrue(projection.advance(generation: firstGeneration))

        let restartedGeneration = try XCTUnwrap(projection.restart())
        XCTAssertEqual(projection.dataset.attempts.first?.phase, .cancelled)
        XCTAssertEqual(projection.dataset.currentAttempt?.kind, .restart)
        XCTAssertEqual(projection.dataset.currentAttempt?.downloadedBytes, 0)

        let beforeStaleCompletion = projection.dataset
        XCTAssertFalse(projection.advance(generation: firstGeneration))
        XCTAssertEqual(projection.dataset, beforeStaleCompletion)
        try advanceUntilReady(&projection, generation: restartedGeneration)
    }

    func testChecksumFailureNeverStagesAndRetrySucceedsOnlyOnce() throws {
        var projection = TutorialNotebookPrototypeFixtureAdapter.make(scenario: .checksumFailure)
        let firstGeneration = try beginApprovedAttempt(&projection)

        XCTAssertTrue(projection.advance(generation: firstGeneration))
        XCTAssertTrue(projection.advance(generation: firstGeneration))
        XCTAssertEqual(projection.dataset.phase, .verifying)
        XCTAssertTrue(projection.advance(generation: firstGeneration))
        XCTAssertEqual(projection.dataset.phase, .checksumFailed)
        XCTAssertFalse(projection.dataset.isStaged)
        XCTAssertFalse(projection.dataset.isReady)
        XCTAssertFalse(projection.advance(generation: firstGeneration))

        let retryGeneration = try XCTUnwrap(projection.retry())
        XCTAssertEqual(projection.dataset.currentAttempt?.kind, .retry)
        XCTAssertEqual(projection.dataset.currentAttempt?.resumeOffsetBytes, 0)
        try advanceUntilReady(&projection, generation: retryGeneration)
        XCTAssertEqual(
            projection.dataset.attempts.filter { $0.phase == .checksumFailed }.count,
            1
        )
    }

    func testDiskFailureRequiresMakeSpaceBeforeRetry() throws {
        var projection = TutorialNotebookPrototypeFixtureAdapter.make(scenario: .diskFailure)
        XCTAssertFalse(projection.approvalFacts.hasEnoughDisk)
        let failedGeneration = try beginApprovedAttempt(&projection)

        XCTAssertEqual(projection.dataset.phase, .diskFailed)
        XCTAssertFalse(projection.dataset.isStaged)
        XCTAssertFalse(projection.dataset.isReady)
        XCTAssertNil(projection.retry())

        let retryGeneration = try XCTUnwrap(projection.makeSpaceAndRetry())
        XCTAssertGreaterThan(retryGeneration, failedGeneration)
        XCTAssertTrue(projection.approvalFacts.hasEnoughDisk)
        XCTAssertEqual(projection.dataset.currentAttempt?.kind, .makeSpaceRetry)
        XCTAssertFalse(projection.advance(generation: failedGeneration))
        try advanceUntilReady(&projection, generation: retryGeneration)
    }

    func testOfflineFailurePreservesPartialProgressWithoutStaging() throws {
        var projection = TutorialNotebookPrototypeFixtureAdapter.make(scenario: .offline)
        let failedGeneration = try beginApprovedAttempt(&projection)

        XCTAssertTrue(projection.advance(generation: failedGeneration))
        XCTAssertEqual(projection.dataset.phase, .offline)
        XCTAssertEqual(
            projection.dataset.currentAttempt?.downloadedBytes,
            TutorialNotebookPrototypeFixtureAdapter.expectedSizeBytes / 4
        )
        XCTAssertFalse(projection.dataset.isStaged)
        XCTAssertFalse(projection.dataset.isReady)

        let retryGeneration = try XCTUnwrap(projection.retry())
        XCTAssertEqual(
            projection.dataset.currentAttempt?.resumeOffsetBytes,
            TutorialNotebookPrototypeFixtureAdapter.expectedSizeBytes / 4
        )
        try advanceUntilReady(&projection, generation: retryGeneration)
        XCTAssertEqual(projection.dataset.attempts.filter { $0.phase == .offline }.count, 1)
    }

    func testUnsafeArchiveFailureNeverStagesAndOneShotRetryRecovers() throws {
        var projection = TutorialNotebookPrototypeFixtureAdapter.make(scenario: .unsafeArchive)
        let failedGeneration = try beginApprovedAttempt(&projection)

        XCTAssertTrue(projection.advance(generation: failedGeneration))
        XCTAssertTrue(projection.advance(generation: failedGeneration))
        XCTAssertTrue(projection.advance(generation: failedGeneration))
        XCTAssertEqual(projection.dataset.phase, .unpacking)
        XCTAssertTrue(projection.advance(generation: failedGeneration))
        XCTAssertEqual(projection.dataset.phase, .unsafeArchive)
        XCTAssertFalse(projection.dataset.isStaged)
        XCTAssertFalse(projection.dataset.isReady)
        XCTAssertFalse(projection.advance(generation: failedGeneration))

        let retryGeneration = try XCTUnwrap(projection.retry())
        try advanceUntilReady(&projection, generation: retryGeneration)
        XCTAssertEqual(
            projection.dataset.attempts.filter { $0.phase == .unsafeArchive }.count,
            1
        )
    }

    func testSectionSelectionAndAcquisitionPreserveLearnerDraftAndTask() throws {
        var projection = TutorialNotebookPrototypeFixtureAdapter.make(scenario: .happyPath)
        let markdown = projection.learnerNotebook.draftMarkdown
        let fixtureTask = projection.fixtureTask

        XCTAssertFalse(projection.selectSection(id: "tutorial-section-inspect"))
        XCTAssertEqual(projection.selectedSectionID, "tutorial-section-acquisition")

        let generation = try beginApprovedAttempt(&projection)
        try advanceUntilReady(&projection, generation: generation)
        XCTAssertEqual(
            projection.sections.first { $0.id == "tutorial-section-acquisition" }?.status,
            .completed
        )
        XCTAssertEqual(
            projection.sections.first { $0.id == "tutorial-section-inspect" }?.status,
            .notStarted
        )
        XCTAssertTrue(projection.selectSection(id: "tutorial-section-image"))
        XCTAssertEqual(projection.selectedSection?.status, .inProgress)
        XCTAssertEqual(projection.learnerNotebook.draftMarkdown, markdown)
        XCTAssertEqual(projection.fixtureTask, fixtureTask)
        XCTAssertEqual(projection.learnerNotebook.receipts, [fixtureTask])
    }

    private func beginApprovedAttempt(
        _ projection: inout TutorialNotebookPrototypeProjection
    ) throws -> Int {
        XCTAssertTrue(projection.showApproval())
        return try XCTUnwrap(projection.approve())
    }

    private func advanceUntilReady(
        _ projection: inout TutorialNotebookPrototypeProjection,
        generation: Int
    ) throws {
        var steps = 0
        while projection.dataset.phase != .ready, steps < 8 {
            XCTAssertTrue(projection.advance(generation: generation))
            steps += 1
        }
        XCTAssertEqual(projection.dataset.phase, .ready)
        XCTAssertTrue(projection.dataset.isStaged)
        XCTAssertTrue(projection.dataset.isReady)
    }
}
