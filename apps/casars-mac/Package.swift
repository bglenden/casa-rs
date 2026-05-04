// swift-tools-version: 5.9

import PackageDescription

let package = Package(
    name: "casars-mac",
    platforms: [
        .macOS(.v13)
    ],
    products: [
        .library(
            name: "CasarsMacCore",
            targets: ["CasarsMacCore"]
        ),
        .executable(
            name: "casars-mac",
            targets: ["CasarsMacApp"]
        )
    ],
    targets: [
        .target(
            name: "CasarsMacCore"
        ),
        .executableTarget(
            name: "CasarsMacApp",
            dependencies: ["CasarsMacCore"]
        ),
        .testTarget(
            name: "CasarsMacCoreTests",
            dependencies: ["CasarsMacCore"]
        )
    ]
)
