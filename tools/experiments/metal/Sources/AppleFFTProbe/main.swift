// SPDX-License-Identifier: LGPL-3.0-or-later

import Foundation
import Metal
import MetalPerformanceShaders
import MetalPerformanceShadersGraph
import ObjectiveC.runtime

private enum Backend: String {
    case mpsGraph = "mpsgraph"
    case mpsNDArrayPrivate = "mpsndarray-private"
    case both
}

private struct Options {
    var rows = 2048
    var columns = 2048
    var batch = 1
    var repeatCount = 10
    var validate = false
    var backend = Backend.mpsGraph
    var inverse = false
    var includeExport = false
}

private struct Complex {
    var re: Double
    var im: Double
}

private func parseOptions() throws -> Options {
    var options = Options()
    var args = CommandLine.arguments.dropFirst().makeIterator()
    while let arg = args.next() {
        switch arg {
        case "--rows":
            guard let value = args.next(), let parsed = Int(value), parsed > 0 else {
                throw ProbeError.message("missing positive integer after --rows")
            }
            options.rows = parsed
        case "--columns":
            guard let value = args.next(), let parsed = Int(value), parsed > 0 else {
                throw ProbeError.message("missing positive integer after --columns")
            }
            options.columns = parsed
        case "--batch":
            guard let value = args.next(), let parsed = Int(value), parsed > 0 else {
                throw ProbeError.message("missing positive integer after --batch")
            }
            options.batch = parsed
        case "--repeat":
            guard let value = args.next(), let parsed = Int(value), parsed > 0 else {
                throw ProbeError.message("missing positive integer after --repeat")
            }
            options.repeatCount = parsed
        case "--validate":
            options.validate = true
        case "--inverse":
            options.inverse = true
        case "--include-export":
            options.includeExport = true
        case "--backend":
            guard let value = args.next(), let parsed = Backend(rawValue: value) else {
                throw ProbeError.message("missing backend after --backend; expected mpsgraph, mpsndarray-private, or both")
            }
            options.backend = parsed
        case "-h", "--help":
            throw ProbeError.message("""
            Usage: swift run AppleFFTProbe [--rows N] [--columns N] [--batch N] [--repeat N] [--inverse] [--include-export] [--validate] [--backend mpsgraph|mpsndarray-private|both]
            """)
        default:
            throw ProbeError.message("unknown argument \(arg)")
        }
    }
    return options
}

private enum ProbeError: Error, CustomStringConvertible {
    case message(String)

    var description: String {
        switch self {
        case let .message(message):
            return message
        }
    }
}

private func deterministicInput(batch: Int, rows: Int, columns: Int) -> [Float] {
    var values = Array(repeating: Float(0), count: batch * rows * columns * 2)
    for plane in 0..<batch {
        for row in 0..<rows {
            for column in 0..<columns {
                var seed = UInt64(plane + 1) &* 0xD6E8_FD50_5AC9_36D5
                seed ^= UInt64(row + 1) &* 0x9E37_79B9_7F4A_7C15
                seed ^= UInt64(column + 1) &* 0xBF58_476D_1CE4_E5B9
                seed ^= seed >> 30
                seed &*= 0xBF58_476D_1CE4_E5B9
                seed ^= seed >> 27
                let base = ((plane * rows * columns) + row * columns + column) * 2
                let reBits = UInt32(truncatingIfNeeded: seed)
                let imBits = UInt32(truncatingIfNeeded: seed >> 32)
                values[base] = Float(Double(reBits) / Double(UInt32.max) * 2.0 - 1.0)
                values[base + 1] = Float(Double(imBits) / Double(UInt32.max) * 2.0 - 1.0)
            }
        }
    }
    return values
}

private func naiveDft(_ input: [Float], rows: Int, columns: Int, inverse: Bool) -> [Complex] {
    let twoPi = 2.0 * Double.pi
    let sign = inverse ? 1.0 : -1.0
    let scale = inverse ? 1.0 / Double(rows * columns) : 1.0
    var output = Array(repeating: Complex(re: 0.0, im: 0.0), count: rows * columns)
    for u in 0..<rows {
        for v in 0..<columns {
            var sumRe = 0.0
            var sumIm = 0.0
            for x in 0..<rows {
                for y in 0..<columns {
                    let inputIndex = (x * columns + y) * 2
                    let valueRe = Double(input[inputIndex])
                    let valueIm = Double(input[inputIndex + 1])
                    let angle = sign * twoPi * (Double(u * x) / Double(rows) + Double(v * y) / Double(columns))
                    let c = cos(angle)
                    let s = sin(angle)
                    sumRe += valueRe * c - valueIm * s
                    sumIm += valueRe * s + valueIm * c
                }
            }
            output[u * columns + v] = Complex(re: sumRe * scale, im: sumIm * scale)
        }
    }
    return output
}

private func maxAbsDifference(_ lhs: [Float], _ rhs: [Complex]) -> Double {
    var maxDiff = 0.0
    for index in 0..<rhs.count {
        let reDiff = abs(Double(lhs[index * 2]) - rhs[index].re)
        let imDiff = abs(Double(lhs[index * 2 + 1]) - rhs[index].im)
        maxDiff = max(maxDiff, reDiff, imDiff)
    }
    return maxDiff
}

private func maxAbsDifferenceBatch(_ lhs: [Float], _ input: [Float], batch: Int, rows: Int, columns: Int, inverse: Bool) -> Double {
    let planeFloatCount = rows * columns * 2
    var maxDiff = 0.0
    for plane in 0..<batch {
        let start = plane * planeFloatCount
        let end = start + planeFloatCount
        let reference = naiveDft(Array(input[start..<end]), rows: rows, columns: columns, inverse: inverse)
        maxDiff = max(maxDiff, maxAbsDifference(Array(lhs[start..<end]), reference))
    }
    return maxDiff
}

private func readResult(
    _ result: MPSGraphTensorData,
    device: MTLDevice,
    queue: MTLCommandQueue,
    byteCount: Int
) throws -> [Float] {
    guard let buffer = device.makeBuffer(length: byteCount, options: [.storageModeShared]) else {
        throw ProbeError.message("failed to allocate shared result buffer")
    }
    let ndarray = result.mpsndarray()
    guard let commandBuffer = queue.makeCommandBuffer() else {
        throw ProbeError.message("failed to create result export command buffer")
    }
    ndarray.exportData(with: commandBuffer, to: buffer, destinationDataType: .complexFloat32, offset: 0, rowStrides: nil)
    commandBuffer.commit()
    commandBuffer.waitUntilCompleted()
    if let error = commandBuffer.error {
        throw ProbeError.message("result export failed: \(error)")
    }
    let pointer = buffer.contents().bindMemory(to: Float.self, capacity: byteCount / MemoryLayout<Float>.stride)
    return Array(UnsafeBufferPointer(start: pointer, count: byteCount / MemoryLayout<Float>.stride))
}

private func timingStats(_ timings: [Double]) -> (avg: Double, std: Double, min: Double, median: Double, max: Double) {
    let avg = timings.reduce(0.0, +) / Double(timings.count)
    let variance = timings.reduce(0.0) { partial, value in
        let diff = value - avg
        return partial + diff * diff
    } / Double(timings.count)
    let sorted = timings.sorted()
    let median: Double
    if sorted.count % 2 == 0 {
        median = (sorted[sorted.count / 2 - 1] + sorted[sorted.count / 2]) / 2.0
    } else {
        median = sorted[sorted.count / 2]
    }
    return (avg, sqrt(variance), sorted[0], median, sorted[sorted.count - 1])
}

private func runMPSGraphProbe(
    options: Options,
    device: MTLDevice,
    queue: MTLCommandQueue,
    input: [Float],
    inputBuffer: MTLBuffer,
    byteCount: Int
) throws {
    let graph = MPSGraph()
    let shape = if options.batch == 1 {
        [NSNumber(value: options.rows), NSNumber(value: options.columns)]
    } else {
        [NSNumber(value: options.batch), NSNumber(value: options.rows), NSNumber(value: options.columns)]
    }
    let placeholder = graph.placeholder(shape: shape, dataType: .complexFloat32, name: "input")
    let descriptor = MPSGraphFFTDescriptor()
    descriptor.inverse = options.inverse
    descriptor.scalingMode = options.inverse ? .size : .none
    let axes: [NSNumber] = if options.batch == 1 {
        [0, 1]
    } else {
        [1, 2]
    }
    let output = graph.fastFourierTransform(placeholder, axes: axes, descriptor: descriptor, name: "fft")
    let tensorData = MPSGraphTensorData(inputBuffer, shape: shape, dataType: .complexFloat32)
    let feeds = [placeholder: tensorData]

    _ = graph.run(with: queue, feeds: feeds, targetTensors: [output], targetOperations: nil)

    let exportBuffer = if options.includeExport {
        device.makeBuffer(length: byteCount, options: [.storageModeShared])
    } else {
        Optional<MTLBuffer>.none
    }
    if options.includeExport && exportBuffer == nil {
        throw ProbeError.message("failed to allocate MPSGraph export buffer")
    }

    var timings: [Double] = []
    var lastResult: MPSGraphTensorData?
    for _ in 0..<options.repeatCount {
        let start = DispatchTime.now().uptimeNanoseconds
        let result = graph.run(with: queue, feeds: feeds, targetTensors: [output], targetOperations: nil)
        if let exportBuffer, options.includeExport {
            guard let resultData = result[output] else {
                throw ProbeError.message("missing MPSGraph result for export")
            }
            guard let commandBuffer = queue.makeCommandBuffer() else {
                throw ProbeError.message("failed to create MPSGraph export command buffer")
            }
            resultData.mpsndarray().exportData(with: commandBuffer, to: exportBuffer, destinationDataType: .complexFloat32, offset: 0, rowStrides: nil)
            commandBuffer.commit()
            commandBuffer.waitUntilCompleted()
            if let error = commandBuffer.error {
                throw ProbeError.message("MPSGraph timed export failed: \(error)")
            }
        }
        let end = DispatchTime.now().uptimeNanoseconds
        timings.append(Double(end - start) / 1_000_000.0)
        lastResult = result[output]
    }

    let stats = timingStats(timings)
    var validation = "\"not_requested\""
    if options.validate {
        guard options.rows * options.columns <= 4096 else {
            throw ProbeError.message("--validate is limited to at most 4096 elements")
        }
        guard let lastResult else {
            throw ProbeError.message("missing result for validation")
        }
        let outputValues = try readResult(lastResult, device: device, queue: queue, byteCount: byteCount)
        validation = String(format: "%.9g", maxAbsDifferenceBatch(outputValues, input, batch: options.batch, rows: options.rows, columns: options.columns, inverse: options.inverse))
    }

    print(String(format: "{\"backend\":\"mpsgraph\",\"precision\":\"f32\",\"direction\":\"%@\",\"rows\":%d,\"columns\":%d,\"batch\":%d,\"repeat\":%d,\"include_export\":%@,\"avg_ms\":%.6f,\"std_ms\":%.6f,\"min_ms\":%.6f,\"median_ms\":%.6f,\"max_ms\":%.6f,\"max_abs_error\":%@,\"device\":\"%@\",\"f64_supported\":false}",
                 options.inverse ? "inverse" : "forward",
                 options.rows,
                 options.columns,
                 options.batch,
                 options.repeatCount,
                 options.includeExport ? "true" : "false",
                 stats.avg,
                 stats.std,
                 stats.min,
                 stats.median,
                 stats.max,
                 validation,
                 device.name))
}

private func makePrivateMPSNDArrayFourierKernel(device: MTLDevice, inverse: Bool) throws -> NSObject {
    guard let kernelClass = NSClassFromString("MPSNDArrayFourierTransform") as? NSObject.Type else {
        throw ProbeError.message("MPSNDArrayFourierTransform is not visible at runtime")
    }
    guard let allocated = class_createInstance(kernelClass, 0) as? NSObject else {
        throw ProbeError.message("failed to allocate MPSNDArrayFourierTransform")
    }
    let selector = NSSelectorFromString("initWithDevice:axesMask:scale:scalingMode:inverse:")
    guard allocated.responds(to: selector), let imp = allocated.method(for: selector) else {
        throw ProbeError.message("MPSNDArrayFourierTransform lacks initWithDevice:axesMask:scale:scalingMode:inverse:")
    }
    typealias InitFn = @convention(c) (NSObject, Selector, AnyObject, UInt, Double, UInt, Bool) -> NSObject
    let initFn = unsafeBitCast(imp, to: InitFn.self)
    let axesMask = UInt(0b11)
    let scalingMode = inverse ? UInt(1) : UInt(0)
    return initFn(allocated, selector, device as AnyObject, axesMask, 1.0, scalingMode, inverse)
}

private func encodePrivateMPSNDArrayFFT(
    kernel: NSObject,
    commandBuffer: MTLCommandBuffer,
    source: MPSNDArray,
    destination: MPSNDArray
) throws {
    let selector = NSSelectorFromString("encodeToCommandBuffer:sourceArray:destinationArray:")
    guard kernel.responds(to: selector), let imp = kernel.method(for: selector) else {
        throw ProbeError.message("MPSNDArrayFourierTransform lacks encodeToCommandBuffer:sourceArray:destinationArray:")
    }
    typealias EncodeFn = @convention(c) (NSObject, Selector, AnyObject, MPSNDArray, MPSNDArray) -> Void
    let encodeFn = unsafeBitCast(imp, to: EncodeFn.self)
    encodeFn(kernel, selector, commandBuffer as AnyObject, source, destination)
}

private func runPrivateMPSNDArrayProbe(
    options: Options,
    device: MTLDevice,
    queue: MTLCommandQueue,
    input: [Float],
    inputBuffer: MTLBuffer,
    byteCount: Int
) throws {
    let shape = if options.batch == 1 {
        [NSNumber(value: options.rows), NSNumber(value: options.columns)]
    } else {
        [NSNumber(value: options.batch), NSNumber(value: options.rows), NSNumber(value: options.columns)]
    }
    let descriptor = MPSNDArrayDescriptor(dataType: .complexFloat32, shape: shape)
    if #available(macOS 15.0, *) {
        descriptor.preferPackedRows = true
    }
    let source = MPSNDArray(buffer: inputBuffer, offset: 0, descriptor: descriptor)
    let destination = MPSNDArray(device: device, descriptor: descriptor)
    let kernel = try makePrivateMPSNDArrayFourierKernel(device: device, inverse: options.inverse)
    let exportBuffer = if options.includeExport {
        device.makeBuffer(length: byteCount, options: [.storageModeShared])
    } else {
        Optional<MTLBuffer>.none
    }
    if options.includeExport && exportBuffer == nil {
        throw ProbeError.message("failed to allocate MPSNDArray export buffer")
    }

    guard let warmup = queue.makeCommandBuffer() else {
        throw ProbeError.message("failed to create MPSNDArray warmup command buffer")
    }
    try encodePrivateMPSNDArrayFFT(kernel: kernel, commandBuffer: warmup, source: source, destination: destination)
    warmup.commit()
    warmup.waitUntilCompleted()
    if let error = warmup.error {
        throw ProbeError.message("MPSNDArray warmup failed: \(error)")
    }

    var timings: [Double] = []
    for _ in 0..<options.repeatCount {
        guard let commandBuffer = queue.makeCommandBuffer() else {
            throw ProbeError.message("failed to create MPSNDArray command buffer")
        }
        let start = DispatchTime.now().uptimeNanoseconds
        try encodePrivateMPSNDArrayFFT(kernel: kernel, commandBuffer: commandBuffer, source: source, destination: destination)
        if let exportBuffer, options.includeExport {
            destination.exportData(with: commandBuffer, to: exportBuffer, destinationDataType: .complexFloat32, offset: 0, rowStrides: nil)
        }
        commandBuffer.commit()
        commandBuffer.waitUntilCompleted()
        let end = DispatchTime.now().uptimeNanoseconds
        if let error = commandBuffer.error {
            throw ProbeError.message("MPSNDArray FFT failed: \(error)")
        }
        timings.append(Double(end - start) / 1_000_000.0)
    }

    let stats = timingStats(timings)
    var validation = "\"not_requested\""
    if options.validate {
        guard options.rows * options.columns <= 4096 else {
            throw ProbeError.message("--validate is limited to at most 4096 elements")
        }
        guard let outputBuffer = device.makeBuffer(length: byteCount, options: [.storageModeShared]) else {
            throw ProbeError.message("failed to allocate MPSNDArray validation buffer")
        }
        guard let commandBuffer = queue.makeCommandBuffer() else {
            throw ProbeError.message("failed to create MPSNDArray validation command buffer")
        }
        destination.exportData(with: commandBuffer, to: outputBuffer, destinationDataType: .complexFloat32, offset: 0, rowStrides: nil)
        commandBuffer.commit()
        commandBuffer.waitUntilCompleted()
        if let error = commandBuffer.error {
            throw ProbeError.message("MPSNDArray export failed: \(error)")
        }
        let pointer = outputBuffer.contents().bindMemory(to: Float.self, capacity: byteCount / MemoryLayout<Float>.stride)
        let outputValues = Array(UnsafeBufferPointer(start: pointer, count: byteCount / MemoryLayout<Float>.stride))
        validation = String(format: "%.9g", maxAbsDifferenceBatch(outputValues, input, batch: options.batch, rows: options.rows, columns: options.columns, inverse: options.inverse))
    }

    print(String(format: "{\"backend\":\"mpsndarray-private\",\"precision\":\"f32\",\"direction\":\"%@\",\"rows\":%d,\"columns\":%d,\"batch\":%d,\"repeat\":%d,\"include_export\":%@,\"avg_ms\":%.6f,\"std_ms\":%.6f,\"min_ms\":%.6f,\"median_ms\":%.6f,\"max_ms\":%.6f,\"max_abs_error\":%@,\"device\":\"%@\",\"f64_supported\":false}",
                 options.inverse ? "inverse" : "forward",
                 options.rows,
                 options.columns,
                 options.batch,
                 options.repeatCount,
                 options.includeExport ? "true" : "false",
                 stats.avg,
                 stats.std,
                 stats.min,
                 stats.median,
                 stats.max,
                 validation,
                 device.name))
}

private func runProbe() throws {
    let options = try parseOptions()
    guard let device = MTLCreateSystemDefaultDevice() else {
        throw ProbeError.message("no Metal device")
    }
    guard let queue = device.makeCommandQueue() else {
        throw ProbeError.message("failed to create Metal command queue")
    }
    let elementCount = options.batch * options.rows * options.columns
    let byteCount = elementCount * 2 * MemoryLayout<Float>.stride
    let input = deterministicInput(batch: options.batch, rows: options.rows, columns: options.columns)
    guard let inputBuffer = device.makeBuffer(bytes: input, length: byteCount, options: [.storageModeShared]) else {
        throw ProbeError.message("failed to allocate input buffer")
    }

    if options.backend == .mpsGraph || options.backend == .both {
        try runMPSGraphProbe(
            options: options,
            device: device,
            queue: queue,
            input: input,
            inputBuffer: inputBuffer,
            byteCount: byteCount
        )
    }
    if options.backend == .mpsNDArrayPrivate || options.backend == .both {
        try runPrivateMPSNDArrayProbe(
            options: options,
            device: device,
            queue: queue,
            input: input,
            inputBuffer: inputBuffer,
            byteCount: byteCount
        )
    }
}

do {
    try runProbe()
} catch {
    fputs("Error: \(error)\n", stderr)
    exit(1)
}
