import XCTest
@testable import KittenLite

class KittenLiteTests: XCTestCase {
    func testExample() throws {
        let database = try Database(atPath: "/Users/joannis/Desktop/database.kl")
        
        let collection = try database.makeCollection(named: "kaas")
        
        XCTAssertEqual(Array(collection).count, 0)
        
        try collection.append([
                "awesome": true
            ])
        
        let documents = Array(collection)
        
        XCTAssertEqual(documents.count, 1)
        
        XCTAssertEqual(Bool(documents[0]["awesome"]), true)
    }

    static var allTests = [
        ("testExample", testExample),
    ]
}
