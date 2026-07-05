import Foundation

public let imagerProgressStderrPrefix = "CASARS_IMAGER_PROGRESS "

public struct ImagerProgressRequest: Codable, Equatable {
    public var taskID: String
    public var runID: String?
    public var taskState: TaskRunState
    public var progress: Double
    public var datasetName: String?
    public var requestSummary: String?

    public init(
        taskID: String,
        runID: String?,
        taskState: TaskRunState,
        progress: Double,
        datasetName: String?,
        requestSummary: String?
    ) {
        self.taskID = taskID
        self.runID = runID
        self.taskState = taskState
        self.progress = progress
        self.datasetName = datasetName
        self.requestSummary = requestSummary
    }
}

public protocol ImagerProgressSource {
    func snapshot(for request: ImagerProgressRequest) -> ImagerProgressSnapshot?
}

public struct EmptyImagerProgressSource: ImagerProgressSource {
    public init() {}

    public func snapshot(for request: ImagerProgressRequest) -> ImagerProgressSnapshot? {
        nil
    }
}

public struct StubImagerProgressSource: ImagerProgressSource {
    public init() {}

    public func snapshot(for request: ImagerProgressRequest) -> ImagerProgressSnapshot? {
        guard request.taskID == "imager" else { return nil }
        return ImagerProgressSnapshot.stub(request: request)
    }
}

public struct ImagerProgressSnapshot: Codable, Equatable {
    public var source: String
    public var runID: String?
    public var state: TaskRunState
    public var phase: String
    public var summary: String
    public var workEstimate: ImagingWorkEstimate
    public var measurementSetWindow: MeasurementSetReadWindowProgress
    public var outputCube: OutputCubeProgress
    public var uvCoverage: UVCoverageProgress
    public var deconvolution: ImagingDeconvolutionProgress
    public var runtime: ImagingRuntimeProgress
    public var observability: ImagingObservabilitySnapshot?
    public var sampledAtLabel: String
    public var sampledAtMilliseconds: UInt64?
    public var receivedAt: Date?

    public init(
        source: String,
        runID: String?,
        state: TaskRunState,
        phase: String,
        summary: String,
        workEstimate: ImagingWorkEstimate,
        measurementSetWindow: MeasurementSetReadWindowProgress,
        outputCube: OutputCubeProgress,
        uvCoverage: UVCoverageProgress,
        deconvolution: ImagingDeconvolutionProgress,
        runtime: ImagingRuntimeProgress,
        observability: ImagingObservabilitySnapshot? = nil,
        sampledAtLabel: String,
        sampledAtMilliseconds: UInt64? = nil,
        receivedAt: Date? = nil
    ) {
        self.source = source
        self.runID = runID
        self.state = state
        self.phase = phase
        self.summary = summary
        self.workEstimate = workEstimate
        self.measurementSetWindow = measurementSetWindow
        self.outputCube = outputCube
        self.uvCoverage = uvCoverage
        self.deconvolution = deconvolution
        self.runtime = runtime
        self.observability = observability
        self.sampledAtLabel = sampledAtLabel
        self.sampledAtMilliseconds = sampledAtMilliseconds
        self.receivedAt = receivedAt
    }

    public static func stub(request: ImagerProgressRequest) -> ImagerProgressSnapshot {
        let running = request.taskState == .running
        let phase = running ? "Major cycle 2: gridding spectral slab" : "Ready: preview telemetry"
        let dataset = request.datasetName ?? "MeasurementSet"
        let measurementSetWindow = MeasurementSetReadWindowProgress(
            totalRows: 84_000,
            totalChannels: 1_024,
            activeRowStart: running ? 18_000 : 0,
            activeRowEnd: running ? 32_000 : 0,
            activeChannelStart: running ? 384 : 0,
            activeChannelEnd: running ? 640 : 0
        )
        let outputCube = OutputCubeProgress(
            xPixels: 2_048,
            yPixels: 2_048,
            zPlanes: 1_024,
            activePlaneStart: running ? 384 : 0,
            activePlaneEnd: running ? 640 : 0
        )
        let deconvolution = ImagingDeconvolutionProgress(
            phase: running ? "Minor cycle" : "Pending clean",
            majorCycle: running ? 2 : 0,
            majorCycleLimit: 6,
            minorIterations: running ? 1_420 : 0,
            minorIterationLimit: 8_000,
            componentsCleaned: running ? 18_600 : 0,
            peakResidualMilliJyPerBeam: running ? 2.7 : 8.8,
            targetResidualMilliJyPerBeam: 1.5,
            residualHistoryMilliJyPerBeam: running
                ? [18.2, 13.6, 10.1, 7.5, 5.2, 3.6, 2.7]
                : [22.0, 17.4, 13.2, 10.4, 8.8]
        )
        return ImagerProgressSnapshot(
            source: "deterministic-stub",
            runID: request.runID,
            state: request.taskState,
            phase: phase,
            summary: "\(dataset) - coarse live-progress mockup",
            workEstimate: ImagingWorkEstimate.stub(
                outputCube: outputCube,
                deconvolution: deconvolution,
                running: running
            ),
            measurementSetWindow: measurementSetWindow,
            outputCube: outputCube,
            uvCoverage: UVCoverageProgress.stub(),
            deconvolution: deconvolution,
            runtime: ImagingRuntimeProgress(
                activeThreads: running ? 14 : 0,
                totalThreads: 16,
                gpuActive: running,
                backend: running ? "CPU + Metal gridding" : "CPU + Metal available",
                sampleCadence: "1 Hz max UI sample",
                memory: ImagingMemoryProgress(
                    memoryTargetBytes: 17_179_869_184,
                    plannedActiveBytes: running ? 15_438_480_384 : 0,
                    sourceStreamBufferBytes: running ? 3_804_104_045 : 0,
                    productScratchBytes: running ? 5_436_915_712 : 0,
                    activePlanes: running ? outputCube.activePlaneCount : 0,
                    rowBlockRows: running ? measurementSetWindow.activeRowCount : 0,
                    memoryTargetSource: "system_half"
                )
            ),
            sampledAtLabel: running ? "live stub" : "idle stub"
        )
    }

    static func live(
        event: ImagerProgressEventPayload,
        runID: String?,
        state: TaskRunState,
        previous: ImagerProgressSnapshot? = nil
    ) -> ImagerProgressSnapshot {
        ImagerProgressSnapshot(
            source: "casars-imager",
            runID: runID ?? previous?.runID,
            state: state,
            phase: event.phase,
            summary: event.summary,
            workEstimate: event.work.map(ImagingWorkEstimate.init(payload:)) ?? previous?.workEstimate ?? ImagingWorkEstimate(
                completedUnits: 0,
                totalUnits: 1,
                unitLabel: "scheduled units",
                basis: "no work estimate in progress event",
                confidence: "unknown"
            ),
            measurementSetWindow: event.msRead.map(MeasurementSetReadWindowProgress.init(payload:)) ?? previous?.measurementSetWindow ?? MeasurementSetReadWindowProgress(
                totalRows: 0,
                totalChannels: 0,
                activeRowStart: 0,
                activeRowEnd: 0,
                activeChannelStart: 0,
                activeChannelEnd: 0
            ),
            outputCube: event.outputCube.map(OutputCubeProgress.init(payload:)) ?? previous?.outputCube ?? OutputCubeProgress(
                xPixels: 1,
                yPixels: 1,
                zPlanes: 1,
                activePlaneStart: 0,
                activePlaneEnd: 0
            ),
            uvCoverage: event.uvCoverage.map(UVCoverageProgress.init(payload:)) ?? previous?.uvCoverage ?? UVCoverageProgress(
                uExtentKilolambda: 0,
                vExtentKilolambda: 0,
                measured: [],
                conjugate: []
            ),
            deconvolution: event.deconvolution.map(ImagingDeconvolutionProgress.init(payload:)) ?? previous?.deconvolution ?? ImagingDeconvolutionProgress(
                phase: event.phase,
                majorCycle: 0,
                majorCycleLimit: 0,
                minorIterations: 0,
                minorIterationLimit: 0,
                componentsCleaned: 0,
                peakResidualMilliJyPerBeam: 0,
                targetResidualMilliJyPerBeam: 0
            ),
            runtime: event.runtime.map(ImagingRuntimeProgress.init(payload:)) ?? previous?.runtime ?? ImagingRuntimeProgress(
                activeThreads: 0,
                totalThreads: 0,
                gpuActive: false,
                backend: "unknown",
                sampleCadence: "event stream"
            ),
            observability: event.observability.map(ImagingObservabilitySnapshot.init(payload:)) ?? previous?.observability,
            sampledAtLabel: Self.elapsedSecondsLabel(milliseconds: event.elapsedMs),
            sampledAtMilliseconds: event.elapsedMs,
            receivedAt: Date()
        )
    }

    public func elapsedLabel(now: Date = Date()) -> String {
        guard state == .running,
              let sampledAtMilliseconds,
              let receivedAt else {
            return sampledAtLabel
        }
        let additionalMilliseconds = UInt64(max(0, now.timeIntervalSince(receivedAt)) * 1_000.0)
        return Self.elapsedSecondsLabel(milliseconds: sampledAtMilliseconds + additionalMilliseconds)
    }

    static func elapsedSecondsLabel(milliseconds: UInt64) -> String {
        let seconds = Double(milliseconds) / 1_000.0
        if seconds < 10 {
            return String(format: "%.2f s", seconds)
        }
        if seconds < 100 {
            return String(format: "%.1f s", seconds)
        }
        return String(format: "%.0f s", seconds)
    }
}

extension ImagerProgressSnapshot {
    public var sourceStreamIsActive: Bool {
        resourceActivities.first { $0.id == "source-stream" }?.isBusy ?? false
    }

    public var resourceActivities: [ImagingResourceActivity] {
        if let observability {
            return observability.resources.map(observedResourceActivity)
        }
        guard let memory = runtime.memory else {
            return []
        }

        let activeThreadBudget = state == .running ? runtime.activeThreads : 0
        let currentPhaseText = phase.lowercased()
        let cyclePhaseText = deconvolution.phase.lowercased()
        let retainedCyclePhaseIsActive = state == .running
            && !cyclePhaseText.isEmpty
            && cyclePhaseText != currentPhaseText
            && !cyclePhaseText.contains("pending")
            && !cyclePhaseText.contains("complete")
            && !cyclePhaseText.contains("dirty imaging")
        let activityPhaseText = retainedCyclePhaseIsActive
            ? "\(currentPhaseText) \(cyclePhaseText)"
            : currentPhaseText
        let sharedPlaneBytes = max(
            0,
            memory.plannedActiveBytes - memory.sourceStreamBufferBytes - memory.productScratchBytes
        )
        let gridBytes = sharedPlaneBytes
        let planeBytes = sharedPlaneBytes
        let deconvolverBytes = sharedPlaneBytes
        let plannedTarget = max(memory.memoryTargetBytes, memory.plannedActiveBytes, 1)

        func phaseBusy(_ keywords: [String], includeRetainedCycle: Bool = true) -> Bool {
            let text = includeRetainedCycle ? activityPhaseText : currentPhaseText
            return state == .running && keywords.contains { text.contains($0) }
        }
        let explicitActiveResources = Set(runtime.activeResourceIDs)
        let ownershipIsAuthoritative = runtime.activeResourceIDsAreAuthoritative
        func resourceBusy(_ id: String, fallback: Bool) -> Bool {
            guard state == .running else { return false }
            return ownershipIsAuthoritative ? explicitActiveResources.contains(id) : fallback
        }
        func resourceThreadCount(_ id: String, fallback: Int) -> Int {
            guard state == .running else { return 0 }
            return max(0, runtime.activeResourceThreadCounts[id] ?? fallback)
        }
        let sourceBusy = resourceBusy(
            "source-stream",
            fallback: phaseBusy(["read", "prepare", "source", "row", "ms"], includeRetainedCycle: false)
        )
        let gridFallbackBusy = phaseBusy(["grid", "fft", "major", "residual", "replay", "dirty image"])
        let deconvolverFallbackBusy = phaseBusy(["minor cycle", "deconvolving", "cleaning", "cycle threshold"])
        let gridBusy = resourceBusy("visibility-grid", fallback: gridFallbackBusy)
        let deconvolverBusy = resourceBusy("deconvolver", fallback: deconvolverFallbackBusy)
        let productBusy = resourceBusy(
            "product-scratch",
            fallback: phaseBusy(["write", "product", "restore", "output", "flush"], includeRetainedCycle: false)
        )
        let planeFallbackBusy = memory.activePlanes > 0 && state == .running && (
            gridBusy || deconvolverBusy || productBusy || phaseBusy(["plane", "slab"])
        )
        let planeBusy = resourceBusy("plane-state", fallback: planeFallbackBusy)

        return [
            ImagingResourceActivity(
                id: "source-stream",
                name: "Source stream",
                detail: Self.resourceDetail(
                    context: "\(Self.compactQuantityLabel(memory.rowBlockRows)) rows",
                    plannedBytes: memory.sourceStreamBufferBytes
                ),
                kind: .source,
                state: sourceBusy ? .busy : .idle,
                residentBytes: memory.sourceStreamBufferBytes,
                targetBytes: plannedTarget,
                sectionStartFraction: measurementSetWindow.rowStartFraction,
                sectionEndFraction: measurementSetWindow.rowEndFraction,
                activeThreads: sourceBusy
                    ? resourceThreadCount("source-stream", fallback: min(activeThreadBudget, max(0, runtime.totalThreads)))
                    : 0,
                totalThreads: runtime.totalThreads,
                gpuActive: false
            ),
            ImagingResourceActivity(
                id: "visibility-grid",
                name: "Grid / FFT",
                detail: Self.resourceDetail(context: "shared", plannedBytes: gridBytes),
                kind: .grid,
                state: gridBusy ? .busy : .idle,
                residentBytes: gridBytes,
                targetBytes: plannedTarget,
                sectionStartFraction: measurementSetWindow.channelStartFraction,
                sectionEndFraction: measurementSetWindow.channelEndFraction,
                activeThreads: gridBusy ? resourceThreadCount("visibility-grid", fallback: activeThreadBudget) : 0,
                totalThreads: runtime.totalThreads,
                gpuActive: runtime.gpuActive && gridBusy
            ),
            ImagingResourceActivity(
                id: "plane-state",
                name: "Plane state",
                detail: Self.resourceDetail(
                    context: Self.planeCountLabel(memory.activePlanes),
                    plannedBytes: planeBytes
                ),
                kind: .plane,
                state: planeBusy ? .busy : .idle,
                residentBytes: planeBytes,
                targetBytes: plannedTarget,
                sectionStartFraction: outputCube.activePlaneStartFraction,
                sectionEndFraction: outputCube.activePlaneEndFraction,
                activeThreads: planeBusy ? resourceThreadCount("plane-state", fallback: activeThreadBudget) : 0,
                totalThreads: runtime.totalThreads,
                gpuActive: runtime.gpuActive && (gridBusy || deconvolverBusy)
            ),
            ImagingResourceActivity(
                id: "deconvolver",
                name: "Deconvolver",
                detail: Self.resourceDetail(context: "minor", plannedBytes: deconvolverBytes),
                kind: .deconvolver,
                state: deconvolverBusy ? .busy : .idle,
                residentBytes: deconvolverBytes,
                targetBytes: plannedTarget,
                sectionStartFraction: 0,
                sectionEndFraction: deconvolution.minorIterationFraction,
                activeThreads: deconvolverBusy ? resourceThreadCount("deconvolver", fallback: activeThreadBudget) : 0,
                totalThreads: runtime.totalThreads,
                gpuActive: runtime.gpuActive && deconvolverBusy
            ),
            ImagingResourceActivity(
                id: "product-scratch",
                name: "Products",
                detail: Self.resourceSizeDetail(plannedBytes: memory.productScratchBytes),
                kind: .product,
                state: productBusy ? .busy : .idle,
                residentBytes: memory.productScratchBytes,
                targetBytes: plannedTarget,
                sectionStartFraction: outputCube.activePlaneStartFraction,
                sectionEndFraction: outputCube.activePlaneEndFraction,
                activeThreads: productBusy ? resourceThreadCount("product-scratch", fallback: min(activeThreadBudget, 2)) : 0,
                totalThreads: runtime.totalThreads,
                gpuActive: false
            )
        ]
    }

    private func observedResourceActivity(_ resource: ImagingObservedResource) -> ImagingResourceActivity {
        let memoryTarget = observability?.memoryTargetBytes
            ?? runtime.memory?.memoryTargetBytes
            ?? max(resource.memory?.plannedBytes ?? 0, resource.memory?.residentBytes ?? 0, 1)
        let bytes = resource.memory?.residentBytes ?? resource.memory?.plannedBytes ?? 0
        let busy = state == .running && resource.state == "busy"
        return ImagingResourceActivity(
            id: resource.id,
            name: resource.label,
            detail: Self.observedResourceDetail(resource),
            kind: Self.resourceKind(for: resource.id),
            state: busy ? .busy : .idle,
            residentBytes: bytes,
            targetBytes: memoryTarget,
            sectionStartFraction: Self.resourceSection(for: resource.id, snapshot: self).0,
            sectionEndFraction: Self.resourceSection(for: resource.id, snapshot: self).1,
            activeThreads: busy ? resource.activeThreads : 0,
            totalThreads: runtime.totalThreads,
            gpuActive: busy && resource.gpuActive
        )
    }

    private static func observedResourceDetail(_ resource: ImagingObservedResource) -> String {
        var parts: [String] = []
        if let rows = resource.memory?.rowBlockRows {
            parts.append("\(compactQuantityLabel(rows)) rows")
        }
        if let planes = resource.memory?.activePlanes {
            parts.append(planeCountLabel(planes))
        }
        let residentBytes = resource.memory?.residentBytes ?? 0
        let plannedBytes = resource.memory?.plannedBytes ?? 0
        if residentBytes > 0, plannedBytes > 0 {
            parts.append("\(compactByteSizeLabel(residentBytes)) now / \(compactByteSizeLabel(plannedBytes)) planned")
        } else if residentBytes > 0 {
            parts.append("\(compactByteSizeLabel(residentBytes)) now")
        } else if plannedBytes > 0 {
            parts.append("\(compactByteSizeLabel(plannedBytes)) planned")
        }
        if parts.isEmpty, let owner = resource.owner, !owner.isEmpty {
            parts.append(owner)
        }
        return parts.isEmpty ? "observed" : parts.joined(separator: " / ")
    }

    private static func resourceKind(for id: String) -> ImagingResourceActivityKind {
        switch id {
        case "source-stream":
            return .source
        case "visibility-grid":
            return .grid
        case "plane-state":
            return .plane
        case "deconvolver":
            return .deconvolver
        case "product-scratch":
            return .product
        default:
            return .grid
        }
    }

    private static func resourceSection(
        for id: String,
        snapshot: ImagerProgressSnapshot
    ) -> (Double, Double) {
        switch id {
        case "source-stream":
            return (
                snapshot.measurementSetWindow.rowStartFraction,
                snapshot.measurementSetWindow.rowEndFraction
            )
        case "visibility-grid":
            return (
                snapshot.measurementSetWindow.channelStartFraction,
                snapshot.measurementSetWindow.channelEndFraction
            )
        case "plane-state", "product-scratch":
            return (
                snapshot.outputCube.activePlaneStartFraction,
                snapshot.outputCube.activePlaneEndFraction
            )
        case "deconvolver":
            return (0, snapshot.deconvolution.minorIterationFraction)
        default:
            return (0, 0)
        }
    }

    private static func resourceDetail(context: String, plannedBytes: Int) -> String {
        guard plannedBytes > 0 else {
            return context
        }
        return "\(context) / \(compactByteSizeLabel(plannedBytes)) planned"
    }

    private static func resourceSizeDetail(plannedBytes: Int) -> String {
        guard plannedBytes > 0 else {
            return "unplanned"
        }
        return "\(compactByteSizeLabel(plannedBytes)) planned"
    }

    private static func planeCountLabel(_ count: Int) -> String {
        count == 1 ? "1 plane" : "\(compactQuantityLabel(count)) planes"
    }

    private static func compactQuantityLabel(_ value: Int) -> String {
        let absolute = abs(value)
        if absolute >= 1_000_000 {
            return String(format: "%.1fM", Double(value) / 1_000_000.0)
        }
        if absolute >= 1_000 {
            return String(format: "%.1fk", Double(value) / 1_000.0)
        }
        return "\(value)"
    }

    private static func compactByteSizeLabel(_ bytes: Int) -> String {
        let absolute = abs(bytes)
        if absolute >= 1_000_000_000 {
            return String(format: "%.1f GB", Double(bytes) / 1_000_000_000.0)
        }
        if absolute >= 1_000_000 {
            return String(format: "%.0f MB", Double(bytes) / 1_000_000.0)
        }
        if absolute >= 1_000 {
            return String(format: "%.0f kB", Double(bytes) / 1_000.0)
        }
        return "\(bytes) B"
    }
}

public enum ImagingResourceActivityState: String, Codable, Equatable {
    case busy
    case idle
}

public enum ImagingResourceActivityKind: String, Codable, Equatable {
    case source
    case grid
    case plane
    case deconvolver
    case product
}

public struct ImagingResourceActivity: Codable, Equatable, Identifiable {
    public var id: String
    public var name: String
    public var detail: String
    public var kind: ImagingResourceActivityKind
    public var state: ImagingResourceActivityState
    public var residentBytes: Int
    public var targetBytes: Int
    public var sectionStartFraction: Double
    public var sectionEndFraction: Double
    public var activeThreads: Int
    public var totalThreads: Int
    public var gpuActive: Bool

    public init(
        id: String,
        name: String,
        detail: String,
        kind: ImagingResourceActivityKind,
        state: ImagingResourceActivityState,
        residentBytes: Int,
        targetBytes: Int,
        sectionStartFraction: Double,
        sectionEndFraction: Double,
        activeThreads: Int,
        totalThreads: Int,
        gpuActive: Bool
    ) {
        self.id = id
        self.name = name
        self.detail = detail
        self.kind = kind
        self.state = state
        self.residentBytes = residentBytes
        self.targetBytes = max(1, targetBytes)
        self.sectionStartFraction = min(1, max(0, sectionStartFraction))
        self.sectionEndFraction = min(1, max(self.sectionStartFraction, sectionEndFraction))
        self.activeThreads = max(0, activeThreads)
        self.totalThreads = max(0, totalThreads)
        self.gpuActive = gpuActive
    }

    public var isBusy: Bool {
        state == .busy
    }

    public var byteFraction: Double {
        fraction(residentBytes, total: targetBytes)
    }
}

public struct ImagingObservabilitySnapshot: Codable, Equatable {
    public var schemaVersion: UInt64
    public var resources: [ImagingObservedResource]
    public var activeSpans: [ImagingObservabilitySpan]
    public var memoryTargetBytes: Int?
    public var memoryTargetSource: String?

    public init(
        schemaVersion: UInt64,
        resources: [ImagingObservedResource],
        activeSpans: [ImagingObservabilitySpan],
        memoryTargetBytes: Int?,
        memoryTargetSource: String?
    ) {
        self.schemaVersion = schemaVersion
        self.resources = resources
        self.activeSpans = activeSpans
        self.memoryTargetBytes = memoryTargetBytes
        self.memoryTargetSource = memoryTargetSource
    }
}

public struct ImagingObservedResource: Codable, Equatable, Identifiable {
    public var id: String
    public var label: String
    public var state: String
    public var leaseCount: Int
    public var activeThreads: Int
    public var gpuActive: Bool
    public var owner: String?
    public var memory: ImagingObservedResourceMemory?

    public init(
        id: String,
        label: String,
        state: String,
        leaseCount: Int,
        activeThreads: Int,
        gpuActive: Bool,
        owner: String?,
        memory: ImagingObservedResourceMemory?
    ) {
        self.id = id
        self.label = label
        self.state = state
        self.leaseCount = leaseCount
        self.activeThreads = activeThreads
        self.gpuActive = gpuActive
        self.owner = owner
        self.memory = memory
    }
}

public struct ImagingObservedResourceMemory: Codable, Equatable {
    public var residentBytes: Int?
    public var plannedBytes: Int?
    public var rowBlockRows: Int?
    public var activePlanes: Int?

    public init(
        residentBytes: Int?,
        plannedBytes: Int?,
        rowBlockRows: Int?,
        activePlanes: Int?
    ) {
        self.residentBytes = residentBytes
        self.plannedBytes = plannedBytes
        self.rowBlockRows = rowBlockRows
        self.activePlanes = activePlanes
    }
}

public struct ImagingObservabilitySpan: Codable, Equatable, Identifiable {
    public var id: String
    public var name: String
    public var state: String
    public var resourceIDs: [String]
    public var elapsedMilliseconds: UInt64

    public init(
        id: String,
        name: String,
        state: String,
        resourceIDs: [String],
        elapsedMilliseconds: UInt64
    ) {
        self.id = id
        self.name = name
        self.state = state
        self.resourceIDs = resourceIDs
        self.elapsedMilliseconds = elapsedMilliseconds
    }
}

public struct ImagingWorkEstimate: Codable, Equatable {
    public var completedUnits: UInt64
    public var totalUnits: UInt64
    public var unitLabel: String
    public var basis: String
    public var confidence: String

    public init(
        completedUnits: UInt64,
        totalUnits: UInt64,
        unitLabel: String,
        basis: String,
        confidence: String
    ) {
        self.completedUnits = completedUnits
        self.totalUnits = totalUnits
        self.unitLabel = unitLabel
        self.basis = basis
        self.confidence = confidence
    }

    public var fraction: Double {
        guard totalUnits > 0 else { return 0 }
        return min(1, Double(completedUnits) / Double(totalUnits))
    }

    public var unitsLabel: String {
        "\(completedUnits) / \(totalUnits) \(unitLabel)"
    }

    public static func stub(
        outputCube: OutputCubeProgress,
        deconvolution: ImagingDeconvolutionProgress,
        running: Bool
    ) -> ImagingWorkEstimate {
        let planeUnits = UInt64(max(0, outputCube.zPlanes))
        let minorUnits = UInt64(max(0, deconvolution.majorCycleLimit))
            * UInt64(max(0, deconvolution.minorIterationLimit))
        let totalUnits = planeUnits + minorUnits
        let basis = "output-plane midpoint plus upper-bound minor iterations"
        guard running, totalUnits > 0 else {
            return ImagingWorkEstimate(
                completedUnits: 0,
                totalUnits: totalUnits,
                unitLabel: "scheduled units",
                basis: basis,
                confidence: "estimate"
            )
        }

        let activePlaneMidpoint = UInt64(max(0, outputCube.activePlaneStart + outputCube.activePlaneCount / 2))
        let completedMajorCycles = UInt64(max(0, deconvolution.majorCycle - 1))
        let completedMinorIterations = completedMajorCycles * UInt64(max(0, deconvolution.minorIterationLimit))
            + UInt64(max(0, deconvolution.minorIterations))
        let completedUnits = activePlaneMidpoint + completedMinorIterations
        return ImagingWorkEstimate(
            completedUnits: min(completedUnits, totalUnits),
            totalUnits: totalUnits,
            unitLabel: "scheduled units",
            basis: basis,
            confidence: "estimate"
        )
    }
}

extension ImagingWorkEstimate {
    init(payload: ImagerProgressWorkPayload) {
        self.init(
            completedUnits: payload.completedUnits,
            totalUnits: payload.totalUnits,
            unitLabel: payload.unitLabel,
            basis: payload.basis,
            confidence: payload.confidence
        )
    }
}

public struct MeasurementSetReadWindowProgress: Codable, Equatable {
    public var totalRows: Int
    public var totalChannels: Int
    public var activeRowStart: Int
    public var activeRowEnd: Int
    public var activeChannelStart: Int
    public var activeChannelEnd: Int

    public init(
        totalRows: Int,
        totalChannels: Int,
        activeRowStart: Int,
        activeRowEnd: Int,
        activeChannelStart: Int,
        activeChannelEnd: Int
    ) {
        self.totalRows = totalRows
        self.totalChannels = totalChannels
        self.activeRowStart = activeRowStart
        self.activeRowEnd = activeRowEnd
        self.activeChannelStart = activeChannelStart
        self.activeChannelEnd = activeChannelEnd
    }

    public var rowStartFraction: Double { fraction(activeRowStart, total: totalRows) }
    public var rowEndFraction: Double { fraction(activeRowEnd, total: totalRows) }
    public var channelStartFraction: Double { fraction(activeChannelStart, total: totalChannels) }
    public var channelEndFraction: Double { fraction(activeChannelEnd, total: totalChannels) }
    public var activeRowCount: Int { max(0, min(totalRows, activeRowEnd) - max(0, activeRowStart)) }
    public var activeChannelCount: Int { max(0, min(totalChannels, activeChannelEnd) - max(0, activeChannelStart)) }

    public var rangeLabel: String {
        "Rows \(activeRowStart)-\(activeRowEnd) / \(totalRows), channels \(activeChannelStart)-\(activeChannelEnd) / \(totalChannels)"
    }
}

extension MeasurementSetReadWindowProgress {
    init(payload: ImagerProgressMsWindowPayload) {
        self.init(
            totalRows: payload.totalRows,
            totalChannels: payload.totalChannels,
            activeRowStart: payload.rowStart,
            activeRowEnd: payload.rowEnd,
            activeChannelStart: payload.channelStart,
            activeChannelEnd: payload.channelEnd
        )
    }
}

public struct OutputCubeProgress: Codable, Equatable {
    public var xPixels: Int
    public var yPixels: Int
    public var zPlanes: Int
    public var activePlaneStart: Int
    public var activePlaneEnd: Int

    public init(
        xPixels: Int,
        yPixels: Int,
        zPlanes: Int,
        activePlaneStart: Int,
        activePlaneEnd: Int
    ) {
        self.xPixels = xPixels
        self.yPixels = yPixels
        self.zPlanes = zPlanes
        self.activePlaneStart = activePlaneStart
        self.activePlaneEnd = activePlaneEnd
    }

    public var activePlaneStartFraction: Double { fraction(activePlaneStart, total: zPlanes) }
    public var activePlaneEndFraction: Double { fraction(activePlaneEnd, total: zPlanes) }
    public var activePlaneCount: Int { max(0, min(zPlanes, activePlaneEnd) - max(0, activePlaneStart)) }
    public var activeRangeSpansWholeXYPlanes: Bool { activePlaneCount > 0 && xPixels > 0 && yPixels > 0 }
    public var zAxisDisplayScale: Double {
        let imageAxis = max(xPixels, yPixels)
        guard imageAxis > 0 else { return 1 }
        return min(0.65, max(0.32, Double(zPlanes) / Double(imageAxis)))
    }

    public var aspectLabel: String {
        "\(xPixels) x \(yPixels) x \(zPlanes) (X x Y x Freq)"
    }

    public var activeRangeLabel: String {
        "Freq planes \(activePlaneStart)-\(activePlaneEnd) / \(zPlanes) (\(activePlaneCount) planes)"
    }
}

extension OutputCubeProgress {
    init(payload: ImagerProgressCubePayload) {
        self.init(
            xPixels: payload.xPixels,
            yPixels: payload.yPixels,
            zPlanes: payload.zPlanes,
            activePlaneStart: payload.activePlaneStart,
            activePlaneEnd: payload.activePlaneEnd
        )
    }
}

public struct UVCoverageProgress: Codable, Equatable {
    public var uExtentKilolambda: Double
    public var vExtentKilolambda: Double
    public var measured: [UVPoint]
    public var conjugate: [UVPoint]
    public var droppedPointCount: UInt64
    public var sampleLimit: Int

    public init(
        uExtentKilolambda: Double,
        vExtentKilolambda: Double,
        measured: [UVPoint],
        conjugate: [UVPoint],
        droppedPointCount: UInt64 = 0,
        sampleLimit: Int? = nil
    ) {
        self.uExtentKilolambda = uExtentKilolambda
        self.vExtentKilolambda = vExtentKilolambda
        self.measured = measured
        self.conjugate = conjugate
        self.droppedPointCount = droppedPointCount
        self.sampleLimit = sampleLimit ?? measured.count
    }

    public var retainedMeasuredPointCount: Int {
        measured.count
    }

    public var acceptedMeasuredPointCount: UInt64 {
        UInt64(measured.count) + droppedPointCount
    }

    public var accumulatedPointCount: Int {
        let accepted = acceptedMeasuredPointCount
        return accepted > UInt64(Int.max) ? Int.max : Int(accepted)
    }

    public static func stub() -> UVCoverageProgress {
        let measured = [
            UVPoint(uKilolambda: -62, vKilolambda: 18, weight: 0.82),
            UVPoint(uKilolambda: -48, vKilolambda: 34, weight: 0.71),
            UVPoint(uKilolambda: -31, vKilolambda: 11, weight: 0.93),
            UVPoint(uKilolambda: -18, vKilolambda: 49, weight: 0.55),
            UVPoint(uKilolambda: -4, vKilolambda: 7, weight: 0.88),
            UVPoint(uKilolambda: 12, vKilolambda: 41, weight: 0.61),
            UVPoint(uKilolambda: 27, vKilolambda: -23, weight: 0.79),
            UVPoint(uKilolambda: 39, vKilolambda: 28, weight: 0.74),
            UVPoint(uKilolambda: 54, vKilolambda: -9, weight: 0.68),
            UVPoint(uKilolambda: 70, vKilolambda: 36, weight: 0.58),
            UVPoint(uKilolambda: -68, vKilolambda: -44, weight: 0.47),
            UVPoint(uKilolambda: -36, vKilolambda: -52, weight: 0.65),
            UVPoint(uKilolambda: 6, vKilolambda: -61, weight: 0.86),
            UVPoint(uKilolambda: 33, vKilolambda: -48, weight: 0.52),
            UVPoint(uKilolambda: 61, vKilolambda: -34, weight: 0.69)
        ]
        let conjugate = measured.map { UVPoint(uKilolambda: -$0.uKilolambda, vKilolambda: -$0.vKilolambda, weight: $0.weight) }
        return UVCoverageProgress(
            uExtentKilolambda: 80,
            vExtentKilolambda: 70,
            measured: measured,
            conjugate: conjugate,
            droppedPointCount: 0,
            sampleLimit: measured.count
        )
    }
}

extension UVCoverageProgress {
    init(payload: ImagerProgressUvCoveragePayload) {
        let measured = payload.measured.map(UVPoint.init(payload:))
        let conjugate = payload.conjugate.isEmpty
            ? measured.map { UVPoint(uKilolambda: -$0.uKilolambda, vKilolambda: -$0.vKilolambda, weight: $0.weight) }
            : payload.conjugate.map(UVPoint.init(payload:))
        self.init(
            uExtentKilolambda: payload.uExtentKilolambda,
            vExtentKilolambda: payload.vExtentKilolambda,
            measured: measured,
            conjugate: conjugate,
            droppedPointCount: payload.droppedPointCount,
            sampleLimit: payload.sampleLimit
        )
    }
}

public struct UVPoint: Codable, Equatable, Identifiable {
    public var id: String { "\(uKilolambda):\(vKilolambda):\(weight)" }
    public var uKilolambda: Double
    public var vKilolambda: Double
    public var weight: Double

    public init(uKilolambda: Double, vKilolambda: Double, weight: Double) {
        self.uKilolambda = uKilolambda
        self.vKilolambda = vKilolambda
        self.weight = weight
    }
}

extension UVPoint {
    init(payload: ImagerProgressUvPointPayload) {
        self.init(
            uKilolambda: payload.uKilolambda,
            vKilolambda: payload.vKilolambda,
            weight: Double(payload.weight)
        )
    }
}

public struct ImagingDeconvolutionProgress: Codable, Equatable {
    public var phase: String
    public var majorCycle: Int
    public var majorCycleLimit: Int
    public var minorIterations: Int
    public var minorIterationLimit: Int
    public var componentsCleaned: Int
    public var peakResidualMilliJyPerBeam: Double
    public var targetResidualMilliJyPerBeam: Double
    public var residualHistoryMilliJyPerBeam: [Double]

    public init(
        phase: String,
        majorCycle: Int,
        majorCycleLimit: Int,
        minorIterations: Int,
        minorIterationLimit: Int,
        componentsCleaned: Int,
        peakResidualMilliJyPerBeam: Double,
        targetResidualMilliJyPerBeam: Double,
        residualHistoryMilliJyPerBeam: [Double] = []
    ) {
        self.phase = phase
        self.majorCycle = majorCycle
        self.majorCycleLimit = majorCycleLimit
        self.minorIterations = minorIterations
        self.minorIterationLimit = minorIterationLimit
        self.componentsCleaned = componentsCleaned
        self.peakResidualMilliJyPerBeam = peakResidualMilliJyPerBeam
        self.targetResidualMilliJyPerBeam = targetResidualMilliJyPerBeam
        self.residualHistoryMilliJyPerBeam = residualHistoryMilliJyPerBeam
    }

    public var minorIterationFraction: Double {
        fraction(minorIterations, total: minorIterationLimit)
    }
}

extension ImagingDeconvolutionProgress {
    init(payload: ImagerProgressDeconvolutionPayload) {
        self.init(
            phase: payload.phase,
            majorCycle: payload.majorCycle,
            majorCycleLimit: payload.majorCycleLimit ?? 0,
            minorIterations: payload.minorIterations,
            minorIterationLimit: payload.minorIterationLimit,
            componentsCleaned: payload.componentsCleaned,
            peakResidualMilliJyPerBeam: Double(payload.peakResidualMilliJyPerBeam ?? 0),
            targetResidualMilliJyPerBeam: Double(payload.targetResidualMilliJyPerBeam ?? 0),
            residualHistoryMilliJyPerBeam: payload.residualHistoryMilliJyPerBeam.map(Double.init)
        )
    }
}

public struct ImagingRuntimeProgress: Codable, Equatable {
    public var activeThreads: Int
    public var totalThreads: Int
    public var gpuActive: Bool
    public var backend: String
    public var sampleCadence: String
    public var activeResourceIDs: [String]
    public var activeResourceIDsAreAuthoritative: Bool
    public var activeResourceThreadCounts: [String: Int]
    public var memory: ImagingMemoryProgress?

    public init(
        activeThreads: Int,
        totalThreads: Int,
        gpuActive: Bool,
        backend: String,
        sampleCadence: String,
        activeResourceIDs: [String] = [],
        activeResourceIDsAreAuthoritative: Bool = false,
        activeResourceThreadCounts: [String: Int] = [:],
        memory: ImagingMemoryProgress? = nil
    ) {
        self.activeThreads = activeThreads
        self.totalThreads = totalThreads
        self.gpuActive = gpuActive
        self.backend = backend
        self.sampleCadence = sampleCadence
        self.activeResourceIDs = activeResourceIDs
        self.activeResourceIDsAreAuthoritative = activeResourceIDsAreAuthoritative
        self.activeResourceThreadCounts = activeResourceThreadCounts
        self.memory = memory
    }

    public var activeThreadFraction: Double {
        fraction(activeThreads, total: totalThreads)
    }
}

public struct ImagingMemoryProgress: Codable, Equatable {
    public var memoryTargetBytes: Int
    public var plannedActiveBytes: Int
    public var sourceStreamBufferBytes: Int
    public var productScratchBytes: Int
    public var activePlanes: Int
    public var rowBlockRows: Int
    public var memoryTargetSource: String?

    public init(
        memoryTargetBytes: Int,
        plannedActiveBytes: Int,
        sourceStreamBufferBytes: Int,
        productScratchBytes: Int,
        activePlanes: Int,
        rowBlockRows: Int,
        memoryTargetSource: String?
    ) {
        self.memoryTargetBytes = memoryTargetBytes
        self.plannedActiveBytes = plannedActiveBytes
        self.sourceStreamBufferBytes = sourceStreamBufferBytes
        self.productScratchBytes = productScratchBytes
        self.activePlanes = activePlanes
        self.rowBlockRows = rowBlockRows
        self.memoryTargetSource = memoryTargetSource
    }
}

extension ImagingRuntimeProgress {
    init(payload: ImagerProgressRuntimePayload) {
        self.init(
            activeThreads: payload.activeThreads,
            totalThreads: payload.totalThreads,
            gpuActive: payload.gpuActive,
            backend: payload.backend,
            sampleCadence: "event stream",
            activeResourceIDs: payload.activeResources,
            activeResourceIDsAreAuthoritative: payload.activeResourcesPresent,
            activeResourceThreadCounts: payload.activeResourceThreads,
            memory: payload.memory.map(ImagingMemoryProgress.init(payload:))
        )
    }
}

struct ImagerProgressEventPayload: Decodable, Equatable {
    var schemaVersion: UInt64
    var sequence: UInt64
    var elapsedMs: UInt64
    var phase: String
    var summary: String
    var work: ImagerProgressWorkPayload?
    var msRead: ImagerProgressMsWindowPayload?
    var outputCube: ImagerProgressCubePayload?
    var uvCoverage: ImagerProgressUvCoveragePayload?
    var deconvolution: ImagerProgressDeconvolutionPayload?
    var runtime: ImagerProgressRuntimePayload?
    var observability: ImagerObservabilityPayload?

    enum CodingKeys: String, CodingKey {
        case schemaVersion = "schema_version"
        case sequence
        case elapsedMs = "elapsed_ms"
        case phase
        case summary
        case work
        case msRead = "ms_read"
        case outputCube = "output_cube"
        case uvCoverage = "uv_coverage"
        case deconvolution
        case runtime
        case observability
    }
}

struct ImagerProgressWorkPayload: Decodable, Equatable {
    var completedUnits: UInt64
    var totalUnits: UInt64
    var unitLabel: String
    var basis: String
    var confidence: String

    enum CodingKeys: String, CodingKey {
        case completedUnits = "completed_units"
        case totalUnits = "total_units"
        case unitLabel = "unit_label"
        case basis
        case confidence
    }
}

struct ImagerProgressMsWindowPayload: Decodable, Equatable {
    var totalRows: Int
    var totalChannels: Int
    var rowStart: Int
    var rowEnd: Int
    var channelStart: Int
    var channelEnd: Int

    enum CodingKeys: String, CodingKey {
        case totalRows = "total_rows"
        case totalChannels = "total_channels"
        case rowStart = "row_start"
        case rowEnd = "row_end"
        case channelStart = "channel_start"
        case channelEnd = "channel_end"
    }
}

struct ImagerProgressCubePayload: Decodable, Equatable {
    var xPixels: Int
    var yPixels: Int
    var zPlanes: Int
    var activePlaneStart: Int
    var activePlaneEnd: Int

    enum CodingKeys: String, CodingKey {
        case xPixels = "x_pixels"
        case yPixels = "y_pixels"
        case zPlanes = "z_planes"
        case activePlaneStart = "active_plane_start"
        case activePlaneEnd = "active_plane_end"
    }
}

struct ImagerProgressUvCoveragePayload: Decodable, Equatable {
    var uExtentKilolambda: Double
    var vExtentKilolambda: Double
    var measured: [ImagerProgressUvPointPayload]
    var conjugate: [ImagerProgressUvPointPayload]
    var droppedPointCount: UInt64
    var sampleLimit: Int

    enum CodingKeys: String, CodingKey {
        case uExtentKilolambda = "u_extent_klambda"
        case vExtentKilolambda = "v_extent_klambda"
        case measured
        case conjugate
        case droppedPointCount = "dropped_points"
        case sampleLimit = "sample_limit"
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        uExtentKilolambda = try container.decode(Double.self, forKey: .uExtentKilolambda)
        vExtentKilolambda = try container.decode(Double.self, forKey: .vExtentKilolambda)
        measured = try container.decodeIfPresent([ImagerProgressUvPointPayload].self, forKey: .measured) ?? []
        conjugate = try container.decodeIfPresent([ImagerProgressUvPointPayload].self, forKey: .conjugate) ?? []
        droppedPointCount = try container.decodeIfPresent(UInt64.self, forKey: .droppedPointCount) ?? 0
        sampleLimit = try container.decodeIfPresent(Int.self, forKey: .sampleLimit) ?? measured.count
    }
}

struct ImagerProgressUvPointPayload: Decodable, Equatable {
    var uKilolambda: Double
    var vKilolambda: Double
    var weight: Float

    enum CodingKeys: String, CodingKey {
        case uKilolambda = "u_klambda"
        case vKilolambda = "v_klambda"
        case weight
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        uKilolambda = try container.decode(Double.self, forKey: .uKilolambda)
        vKilolambda = try container.decode(Double.self, forKey: .vKilolambda)
        weight = try container.decodeIfPresent(Float.self, forKey: .weight) ?? 1.0
    }
}

struct ImagerProgressDeconvolutionPayload: Decodable, Equatable {
    var phase: String
    var majorCycle: Int
    var majorCycleLimit: Int?
    var minorIterations: Int
    var minorIterationLimit: Int
    var componentsCleaned: Int
    var peakResidualMilliJyPerBeam: Float?
    var targetResidualMilliJyPerBeam: Float?
    var residualHistoryMilliJyPerBeam: [Float]

    enum CodingKeys: String, CodingKey {
        case phase
        case majorCycle = "major_cycle"
        case majorCycleLimit = "major_cycle_limit"
        case minorIterations = "minor_iterations"
        case minorIterationLimit = "minor_iteration_limit"
        case componentsCleaned = "components_cleaned"
        case peakResidualMilliJyPerBeam = "peak_residual_mjy_per_beam"
        case targetResidualMilliJyPerBeam = "target_residual_mjy_per_beam"
        case residualHistoryMilliJyPerBeam = "residual_history_mjy_per_beam"
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        phase = try container.decode(String.self, forKey: .phase)
        majorCycle = try container.decode(Int.self, forKey: .majorCycle)
        majorCycleLimit = try container.decodeIfPresent(Int.self, forKey: .majorCycleLimit)
        minorIterations = try container.decode(Int.self, forKey: .minorIterations)
        minorIterationLimit = try container.decode(Int.self, forKey: .minorIterationLimit)
        componentsCleaned = try container.decode(Int.self, forKey: .componentsCleaned)
        peakResidualMilliJyPerBeam = try container.decodeIfPresent(Float.self, forKey: .peakResidualMilliJyPerBeam)
        targetResidualMilliJyPerBeam = try container.decodeIfPresent(Float.self, forKey: .targetResidualMilliJyPerBeam)
        residualHistoryMilliJyPerBeam = try container.decodeIfPresent([Float].self, forKey: .residualHistoryMilliJyPerBeam) ?? []
    }
}

struct ImagerProgressRuntimePayload: Decodable, Equatable {
    var activeThreads: Int
    var totalThreads: Int
    var gpuActive: Bool
    var backend: String
    var activeResources: [String]
    var activeResourcesPresent: Bool
    var activeResourceThreads: [String: Int]
    var memory: ImagerProgressMemoryPayload?

    enum CodingKeys: String, CodingKey {
        case activeThreads = "active_threads"
        case totalThreads = "total_threads"
        case gpuActive = "gpu_active"
        case backend
        case activeResources = "active_resources"
        case activeResourceThreads = "active_resource_threads"
        case memory
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        activeThreads = try container.decode(Int.self, forKey: .activeThreads)
        totalThreads = try container.decode(Int.self, forKey: .totalThreads)
        gpuActive = try container.decode(Bool.self, forKey: .gpuActive)
        backend = try container.decode(String.self, forKey: .backend)
        activeResourcesPresent = container.contains(.activeResources)
        activeResources = try container.decodeIfPresent([String].self, forKey: .activeResources) ?? []
        activeResourceThreads = try container.decodeIfPresent([String: Int].self, forKey: .activeResourceThreads) ?? [:]
        memory = try container.decodeIfPresent(ImagerProgressMemoryPayload.self, forKey: .memory)
    }
}

struct ImagerProgressMemoryPayload: Decodable, Equatable {
    var memoryTargetBytes: Int
    var plannedActiveBytes: Int
    var sourceStreamBufferBytes: Int
    var productScratchBytes: Int
    var activePlanes: Int
    var rowBlockRows: Int
    var memoryTargetSource: String?

    enum CodingKeys: String, CodingKey {
        case memoryTargetBytes = "memory_target_bytes"
        case plannedActiveBytes = "planned_active_bytes"
        case sourceStreamBufferBytes = "source_stream_buffer_bytes"
        case productScratchBytes = "product_scratch_bytes"
        case activePlanes = "active_planes"
        case rowBlockRows = "row_block_rows"
        case memoryTargetSource = "memory_target_source"
    }
}

struct ImagerObservabilityPayload: Decodable, Equatable {
    var schemaVersion: UInt64
    var resources: [ImagerObservedResourcePayload]
    var activeSpans: [ImagerObservabilitySpanPayload]
    var memoryTargetBytes: Int?
    var memoryTargetSource: String?

    enum CodingKeys: String, CodingKey {
        case schemaVersion = "schema_version"
        case resources
        case activeSpans = "active_spans"
        case memoryTargetBytes = "memory_target_bytes"
        case memoryTargetSource = "memory_target_source"
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        schemaVersion = try container.decode(UInt64.self, forKey: .schemaVersion)
        resources = try container.decodeIfPresent([ImagerObservedResourcePayload].self, forKey: .resources) ?? []
        activeSpans = try container.decodeIfPresent([ImagerObservabilitySpanPayload].self, forKey: .activeSpans) ?? []
        memoryTargetBytes = try container.decodeIfPresent(Int.self, forKey: .memoryTargetBytes)
        memoryTargetSource = try container.decodeIfPresent(String.self, forKey: .memoryTargetSource)
    }
}

struct ImagerObservedResourcePayload: Decodable, Equatable {
    var id: String
    var label: String
    var state: String
    var leaseCount: Int
    var activeThreads: Int
    var gpuActive: Bool
    var owner: String?
    var memory: ImagerObservedResourceMemoryPayload?

    enum CodingKeys: String, CodingKey {
        case id
        case label
        case state
        case leaseCount = "lease_count"
        case activeThreads = "active_threads"
        case gpuActive = "gpu_active"
        case owner
        case memory
    }
}

struct ImagerObservedResourceMemoryPayload: Decodable, Equatable {
    var residentBytes: Int?
    var plannedBytes: Int?
    var rowBlockRows: Int?
    var activePlanes: Int?

    enum CodingKeys: String, CodingKey {
        case residentBytes = "resident_bytes"
        case plannedBytes = "planned_bytes"
        case rowBlockRows = "row_block_rows"
        case activePlanes = "active_planes"
    }
}

struct ImagerObservabilitySpanPayload: Decodable, Equatable {
    var id: String
    var name: String
    var state: String
    var resourceIDs: [String]
    var elapsedMilliseconds: UInt64

    enum CodingKeys: String, CodingKey {
        case id
        case name
        case state
        case resourceIDs = "resource_ids"
        case elapsedMilliseconds = "elapsed_ms"
    }
}

extension ImagingMemoryProgress {
    init(payload: ImagerProgressMemoryPayload) {
        self.init(
            memoryTargetBytes: payload.memoryTargetBytes,
            plannedActiveBytes: payload.plannedActiveBytes,
            sourceStreamBufferBytes: payload.sourceStreamBufferBytes,
            productScratchBytes: payload.productScratchBytes,
            activePlanes: payload.activePlanes,
            rowBlockRows: payload.rowBlockRows,
            memoryTargetSource: payload.memoryTargetSource
        )
    }
}

extension ImagingObservabilitySnapshot {
    init(payload: ImagerObservabilityPayload) {
        self.init(
            schemaVersion: payload.schemaVersion,
            resources: payload.resources.map(ImagingObservedResource.init(payload:)),
            activeSpans: payload.activeSpans.map(ImagingObservabilitySpan.init(payload:)),
            memoryTargetBytes: payload.memoryTargetBytes,
            memoryTargetSource: payload.memoryTargetSource
        )
    }
}

extension ImagingObservedResource {
    init(payload: ImagerObservedResourcePayload) {
        self.init(
            id: payload.id,
            label: payload.label,
            state: payload.state,
            leaseCount: payload.leaseCount,
            activeThreads: payload.activeThreads,
            gpuActive: payload.gpuActive,
            owner: payload.owner,
            memory: payload.memory.map(ImagingObservedResourceMemory.init(payload:))
        )
    }
}

extension ImagingObservedResourceMemory {
    init(payload: ImagerObservedResourceMemoryPayload) {
        self.init(
            residentBytes: payload.residentBytes,
            plannedBytes: payload.plannedBytes,
            rowBlockRows: payload.rowBlockRows,
            activePlanes: payload.activePlanes
        )
    }
}

extension ImagingObservabilitySpan {
    init(payload: ImagerObservabilitySpanPayload) {
        self.init(
            id: payload.id,
            name: payload.name,
            state: payload.state,
            resourceIDs: payload.resourceIDs,
            elapsedMilliseconds: payload.elapsedMilliseconds
        )
    }
}

enum ImagerProgressStderrRecord: Equatable {
    case progress(ImagerProgressSnapshot)
    case diagnostic(String)
}

struct ImagerProgressStderrParser {
    private var pending = ""
    private var lastProgress: ImagerProgressSnapshot?

    mutating func append(_ text: String, runID: String?, state: TaskRunState) -> [ImagerProgressStderrRecord] {
        pending.append(text)
        var records: [ImagerProgressStderrRecord] = []
        while let newline = pending.firstIndex(of: "\n") {
            let line = String(pending[..<newline])
            pending.removeSubrange(...newline)
            records.append(record(for: line, runID: runID, state: state))
        }
        return records
    }

    mutating func finish(runID: String?, state: TaskRunState) -> [ImagerProgressStderrRecord] {
        guard !pending.isEmpty else { return [] }
        let line = pending
        pending = ""
        return [record(for: line, runID: runID, state: state)]
    }

    private mutating func record(for rawLine: String, runID: String?, state: TaskRunState) -> ImagerProgressStderrRecord {
        let line = rawLine.trimmingCharacters(in: .newlines)
        guard line.hasPrefix(imagerProgressStderrPrefix) else {
            return line.isEmpty ? .diagnostic("") : .diagnostic(line)
        }
        let json = String(line.dropFirst(imagerProgressStderrPrefix.count))
        do {
            let payload = try JSONDecoder().decode(ImagerProgressEventPayload.self, from: Data(json.utf8))
            let snapshot = ImagerProgressSnapshot.live(
                event: payload,
                runID: runID,
                state: state,
                previous: lastProgress
            )
            lastProgress = snapshot
            return .progress(snapshot)
        } catch {
            return .diagnostic("Malformed imager progress event: \(error)")
        }
    }
}

private func fraction(_ value: Int, total: Int) -> Double {
    guard total > 0 else { return 0 }
    return min(1, max(0, Double(value) / Double(total)))
}
