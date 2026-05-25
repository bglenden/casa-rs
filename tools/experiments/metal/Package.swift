// SPDX-License-Identifier: LGPL-3.0-or-later
// swift-tools-version: 6.0

import PackageDescription

let package = Package(
    name: "MetalGridExperiment",
    platforms: [
        .macOS(.v15),
    ],
    targets: [
        .executableTarget(
            name: "MetalGridExperiment",
            linkerSettings: [
                .linkedFramework("Metal"),
            ]
        ),
    ]
)
