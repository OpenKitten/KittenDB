import Foundation
import XCTest
@testable import KittenDB

class KittenLiteTests: XCTestCase {
    func testExample() throws {
        let path = "/Users/joannis/Desktop/database.kdb"
        
        if FileManager.default.fileExists(atPath: path) {
            try FileManager.default.removeItem(atPath: path)
        }
        
        let database = try Database(atPath: path)
        
        let collection = try database.makeCollection(named: "kaas")
        
        XCTAssertEqual(Array(collection).count, 0)
        
        try collection.append([
                "awesome": true
            ])
        
        XCTAssertEqual(try collection.count(), 1)
        
        let documents = Array(collection)
        
        XCTAssertEqual(Bool(documents[0]["awesome"]), true)
        
        try collection.append([
            "awesome": true
            ])
        
        try collection.append([
            "awesome": true
            ])
        
        try collection.append([
            "awesome": true
            ])
        
        XCTAssertEqual(try collection.count(), 4)
        
        for document in collection {
            XCTAssertEqual(Bool(document["awesome"]), true)
        }
        
        try collection.update(["awesome": true], to: ["awesome": false])
        
        XCTAssertEqual(try collection.count(), 4)
        
        for document in collection {
            XCTAssertEqual(Bool(document["awesome"]), false)
        }
        
        try collection.remove(["awesome": false])
        
        XCTAssertEqual(try collection.count(), 0)
    }

    static var allTests = [
        ("testExample", testExample),
    ]
}
