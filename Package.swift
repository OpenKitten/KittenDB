// swift-tools-version:3.1

import PackageDescription

let package = Package(
    name: "KittenDB",
    dependencies: [
        .Package(url: "https://github.com/OpenKitten/BSON.git", majorVersion: 5)
    ]
)
