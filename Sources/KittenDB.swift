import BSON
import Foundation

extension UInt32 {
    internal func makeBytes() -> Bytes {
        let integer = self.littleEndian
        
        return [
            Byte(integer & 0xFF),
            Byte((integer >> 8) & 0xFF),
            Byte((integer >> 16) & 0xFF),
            Byte((integer >> 24) & 0xFF),
        ]
    }
}

extension UInt64 {
    internal func makeBytes() -> Bytes {
        let integer = self.littleEndian
        
        return [
            Byte(integer & 0xFF),
            Byte((integer >> 8) & 0xFF),
            Byte((integer >> 16) & 0xFF),
            Byte((integer >> 24) & 0xFF),
            Byte((integer >> 32) & 0xFF),
            Byte((integer >> 40) & 0xFF),
            Byte((integer >> 48) & 0xFF),
            Byte((integer >> 56) & 0xFF),
        ]
    }
}

public class Database {
    let handle: FileHandle
    let version: UInt32
    var masterPage: MasterPage!
    
    enum Error : Swift.Error {
        case notAccessible(atPath: String)
        case invalidFileStructure
        case invalidPage
        case invalidDocument
        case invalidDocumentReference
    }
    
    public init(atPath filePath: String) throws {
        if !FileManager.default.fileExists(atPath: filePath) {
            guard FileManager.default.createFile(atPath: filePath, contents: nil, attributes: nil) else {
                throw Error.notAccessible(atPath: filePath)
            }
            
            self.handle = try FileHandle(forUpdating: URL(fileURLWithPath: filePath))
            self.version = 1
            self.masterPage = try MasterPage(in: self)
            
            handle.write(Data(self.version.makeBytes()))
            handle.write(self.masterPage.data)
        } else {
            self.handle = try FileHandle(forUpdating: URL(fileURLWithPath: filePath))
            
            self.handle.seek(toFileOffset: 0)
            let versionData = self.handle.readData(ofLength: 4)
            
            guard versionData.count == 4 else {
                throw Error.invalidFileStructure
            }
            
            self.version = UnsafeRawPointer(Array(versionData)).assumingMemoryBound(to: UInt32.self).pointee
            
            self.handle.seek(toFileOffset: 4)
            let masterPageData = self.handle.readData(ofLength: PageLength.small.byteLength)
            
            guard masterPageData.count == PageLength.small.byteLength else {
                throw Error.invalidFileStructure
            }
            
            self.masterPage = try MasterPage(in: self)
        }
    }
    
    func write(_ page: Page) {
        self.handle.seek(toFileOffset: page.filePosition)
        self.handle.write(page.data)
    }
    
    func read(_ range: Range<Int>) -> Data {
        self.handle.seek(toFileOffset: UInt64(range.lowerBound))
        return self.handle.readData(ofLength: range.upperBound - range.lowerBound)
    }
    
    func read(from position: UInt64, length: Int) -> Data {
        self.handle.seek(toFileOffset: position)
        return self.handle.readData(ofLength: length)
    }
    
    func readPage(_ number: UInt32 = 0) -> Page? {
        if number == 0 {
            return masterPage
        }
        
        return nil
    }
}

struct DocumentReference {
    let position: UInt64
    let pagePosition: Int
    let database: Database
    
    func resolve() throws -> Document? {
        let documentLengthBytes = database.read(from: self.position, length: 4)
        
        let documentLength = UnsafeRawPointer(Array(documentLengthBytes)).assumingMemoryBound(to: Int32.self).pointee
        
        let documentBytes = database.read(from: self.position, length: Int(documentLength))
        
        guard documentBytes.count == Int(documentLength) else {
            throw Database.Error.invalidDocument
        }
        
        let document = Document(data: documentBytes)
        
        guard document.validate() else {
            throw Database.Error.invalidDocument
        }
        
        return document
    }
}

struct PageReference {
    let length: PageLength
    let type: PageType
    let position: UInt64
    let database: Database
    
    func resolve() throws -> Page? {
        database.handle.seek(toFileOffset: self.position)
        let bytes = database.handle.readData(ofLength: 2)
        
        guard bytes.count == 2, let length = PageLength(rawValue: bytes[0]), let type = PageType(rawValue: bytes[1]) else {
            throw Database.Error.invalidPage
        }
        
        let position = Int(self.position)
        
        switch type {
        case .master:
            return try MasterPage(database.read(position..<position + length.byteLength), in: database)
        case .collectionHeader:
            return try HeaderCollectionPage(database.read(position..<position + length.byteLength), in: database)
        case .collectionBody:
            return try BodyCollectionPage(database.read(position..<position + length.byteLength), in: database)
        default:
            return nil
        }
    }
    
    init(length: PageLength, type: PageType, position: UInt64, database: Database) {
        self.length = length
        self.type = type
        self.position = position
        self.database = database
    }
    
    init(to page: Page, at position: UInt64) {
        self.length = page.length
        self.type = page.type
        self.position = position
        self.database = page.database
    }
}

enum PageType : UInt8 {
    case unknown
    case master
    case collectionHeader
    case collectionBody
    case index
}

enum PageLength: UInt8 {
    case none
    case small
    case medium
    
    var byteLength: Int {
        switch self {
        case .none:
            return -1
        case .small:
            return 1000
        case .medium:
            return 1000000
        }
    }
}

protocol Page : class {
    var filePosition: UInt64 { get set }
    var database: Database { get }
    var type: PageType { get }
    var data: Data { get set }
    
    func validate() throws
}

extension Page {
    var length: PageLength {
        return PageLength(rawValue: data[0]) ?? .none
    }
    
    var next: UInt64 {
        get {
            return UnsafeRawPointer(Array(self.data[2..<10])).assumingMemoryBound(to: UInt64.self).pointee
        }
        set {
            let bytes = newValue.makeBytes()
            
            self.data[2] = bytes[0]
            self.data[3] = bytes[1]
            self.data[4] = bytes[2]
            self.data[5] = bytes[3]
            self.data[6] = bytes[4]
            self.data[7] = bytes[5]
            self.data[8] = bytes[6]
            self.data[9] = bytes[7]
        }
    }
    
    var nextPage: PageReference? {
        get {
            guard self.next > 0 else {
                return nil
            }
            
            return PageReference(length: PageLength(rawValue: data[0]) ?? .none, type: self.type, position: next, database: database)
        }
        set {
            guard let newValue = newValue else {
                fatalError("You cannot remove pages")
            }
            
            assert(try! newValue.resolve()!.type == self.type, "Pages *must* be already created and of the same type")
            
            self.next = newValue.position
        }
    }
    
    func validate() throws {
        guard data.count >= 10 else {
            throw Database.Error.invalidPage
        }
        
        guard data[0] == self.type.rawValue else {
            throw Database.Error.invalidPage
        }
        
        guard let length = PageLength(rawValue: data[1]), length.byteLength == data.count else {
            throw Database.Error.invalidPage
        }
    }
}

class MasterPage : Page, Sequence {
    var filePosition: UInt64 = 0
    
    var type: PageType {
        return .master
    }

    var database: Database
    var data: Data

    static var empty: Data {
        var data = Data(repeating: 0, count: PageLength.small.byteLength)
        data[0] = PageLength.small.rawValue
        data[1] = PageType.master.rawValue
        
        return data
    }
    
    var nextPage: PageReference? {
        get {
            guard self.next > 0 else {
                return nil
            }
            
            return PageReference(length: PageLength(rawValue: data[0]) ?? .none, type: self.type, position: next, database: database)
        }
        set {
            guard let newValue = newValue else {
                fatalError("You cannot remove pages")
            }
            
            assert(try! newValue.resolve()!.type == .collectionBody, "A MasterCollection page must be succeeded by a Collection page")
            
            self.data[0] = newValue.length.rawValue
            self.data[1] = newValue.type.rawValue
            
            let bytes = newValue.position.makeBytes()
            
            self.data[2] = bytes[0]
            self.data[3] = bytes[1]
            self.data[4] = bytes[2]
            self.data[5] = bytes[3]
            self.data[6] = bytes[4]
            self.data[7] = bytes[5]
            self.data[8] = bytes[6]
            self.data[9] = bytes[7]
        }
    }
    
    init(_ data: Data = MasterPage.empty, in database: Database) throws {
        self.data = data
        self.database = database
        
        try validate()
    }
    
    func makeIterator() -> AnyIterator<PageReference> {
        var i = 10
        
        return AnyIterator {
            defer { i += 10 }
            
            guard i + 10 <= self.data.count else {
                return nil
            }
            
            let length = PageLength(rawValue: self.data[i]) ?? .none
            let type = PageType(rawValue: self.data[i + 1]) ?? .unknown
            
            let position = UnsafeRawPointer(Array(self.data[i + 2..<i + 10])).assumingMemoryBound(to: UInt64.self).pointee
            guard position != 0 else {
                return nil
            }
            
            return PageReference(length: length, type: type, position: position, database: self.database)
        }
    }
    
    func append(_ page: Page) throws {
        if let nextPage = try nextPage?.resolve() {
            guard let nextPage = nextPage as? MasterPage else {
                throw Database.Error.invalidPage
            }
            
            return try nextPage.append(page)
        }
        
        let position = database.handle.seekToEndOfFile()
        database.handle.write(page.data)
        page.filePosition = position
        
        // base offset + (entries * entry length)
        let offset = 10 + (Array(self).count * 10)
        
        // offset + one entry lenght
        guard offset + 10 <= data.count else {
            let masterPage = try MasterPage(in: database)
            
            let nextMasterPosition = database.handle.seekToEndOfFile()
            database.handle.write(masterPage.data)
            masterPage.filePosition = nextMasterPosition
            
            self.nextPage = PageReference(to: masterPage, at: nextMasterPosition)
            
            try masterPage.append(page)
            return
        }
        
        self.data[offset] = page.length.rawValue
        self.data[offset + 1] = page.type.rawValue
        
        for (position, byte) in position.makeBytes().enumerated() {
            self.data[offset + 2 + position] = byte
        }
        
        database.write(self)
    }
}

protocol CollectionPage : Page {
    var firstEntryPosition: Int { get }
}

extension CollectionPage {
    func makeIterator() -> AnyIterator<DocumentReference> {
        var i = firstEntryPosition
        
        return AnyIterator {
            defer { i += 8 }
            
            guard i + 8 < self.data.count else {
                return nil
            }
            
            let position = UnsafeRawPointer(Array(self.data[i..<i + 8])).assumingMemoryBound(to: UInt64.self).pointee
            guard position != 0 else {
                return nil
            }
            
            return DocumentReference(position: position, pagePosition: i, database: self.database)
        }
    }
    
    func remove(_ reference: DocumentReference) throws {
        guard reference.pagePosition > 0 && reference.pagePosition < self.length.byteLength else {
            throw Database.Error.invalidDocumentReference
        }
        
        for i in 0..<8 {
            self.data[reference.pagePosition + i] = 0
        }
        
        database.write(self)
    }
    
    func update(_ reference: DocumentReference, to other: Document) throws {
        guard let referenced = try reference.resolve() else {
            throw Database.Error.invalidDocumentReference
        }
        
        if referenced.byteCount >= other.byteCount {
            database.handle.seek(toFileOffset: reference.position)
            database.handle.write(Data(other.bytes))
        } else {
            let offset = database.handle.seekToEndOfFile()
            database.handle.write(Data(other.bytes))
            
            for (position, byte) in offset.makeBytes().enumerated() {
                self.data[reference.pagePosition + position] = byte
            }
            
            database.write(self)
        }
    }
    
    func append(_ document: Document) throws {
        if let nextPage = try self.nextPage?.resolve() {
            guard let nextPage = nextPage as? BodyCollectionPage else {
                throw Database.Error.invalidPage
            }
            
            return try nextPage.append(document)
        }
        
        let position = database.handle.seekToEndOfFile()
        database.handle.write(Data(document.bytes))
        
        // base offset + (entires * entry length)
        let offset = firstEntryPosition + (Array(self.makeIterator()).count * 8)
        
        // offset + one entry length
        guard offset + 8 <= data.count else {
            let collectionPage = try BodyCollectionPage(in: database)
            
            let nextPagePosition = database.handle.seekToEndOfFile()
            database.handle.write(collectionPage.data)
            collectionPage.filePosition = nextPagePosition
            
            self.nextPage = PageReference(to: collectionPage, at: nextPagePosition)
            
            try collectionPage.append(document)
            return
        }
        
        for (position, byte) in position.makeBytes().enumerated() {
            self.data[offset + position] = byte
        }
        
        database.write(self)
    }
}

class HeaderCollectionPage : CollectionPage, Sequence {
    var filePosition: UInt64 = 0
    var firstEntryPosition: Int {
        return 11 + name.utf8.count
    }
    
    var type: PageType {
        return .collectionHeader
    }
    
    var database: Database
    var data: Data
    
    var name: String
    
    init(_ data: Data, in database: Database) throws {
        guard data.count > 11 else {
            throw Database.Error.invalidPage
        }
        
        let length = Int(data[10])
        
        guard data.count > 11 + length, let name = String(bytes: data[11..<11 + length], encoding: .utf8) else {
            throw Database.Error.invalidPage
        }
        
        self.name = name
        self.data = data
        self.database = database
        
        try validate()
    }
    
    init(named name: String, withLength length: PageLength = PageLength.small, in database: Database) throws {
        var data = Data(repeating: 0, count: length.byteLength)
        data[0] = length.rawValue
        data[1] = PageType.collectionHeader.rawValue
        
        let utf8 = [UInt8](name.utf8)
        data[10] = UInt8(utf8.count)
        
        for (i, byte) in utf8.enumerated() {
            data[i + 11] = byte
        }
        
        self.name = name
        self.data = data
        self.database = database
    }
}

class BodyCollectionPage : CollectionPage, Sequence {
    var filePosition: UInt64 = 0
    let firstEntryPosition = 10

    var type: PageType {
        return .collectionBody
    }
    
    var database: Database
    var data: Data
    
    init(withLength length: PageLength = PageLength.small, in database: Database) throws {
        var data = Data(repeating: 0, count: length.byteLength)
        data[0] = length.rawValue
        data[1] = PageType.collectionBody.rawValue
        
        self.data = data
        self.database = database
    }
    
    init(_ data: Data, in database: Database) throws {
        self.data = data
        self.database = database
        
        try validate()
    }
}

extension Database {
    public func makeCollection(named name: String) throws -> Collection {
        let collectionHeader = try HeaderCollectionPage(named: name, in: self)
        
        try self.masterPage.append(collectionHeader)
        
        return Collection(headerPage: collectionHeader)
    }
}

public class Collection : Sequence {
    var name: String {
        return self.header.name
    }
    
    let header: HeaderCollectionPage
    
    var database: Database {
        return self.header.database
    }
    
    init(headerPage: HeaderCollectionPage) {
        self.header = headerPage
    }
    
    public func append(_ document: Document) throws {
        try self.header.append(document)
    }
    
    @discardableResult
    public func update(_ matching: Document, to other: Document) throws -> Int {
        var count = 0
        
        try self.forEach { pointer, page in
            guard let document = try pointer.resolve() else {
                return false
            }
            
            for (key, value) in matching {
                guard document[key]?.makeBinary() ?? [] == value.makeBinary() else {
                    return true
                }
            }
            
            try page.update(pointer, to: other)
            
            count += 1
            return true
        }
        
        return count
    }
    
    @discardableResult
    public func remove(_ matching: Document) throws -> Int {
        var count = 0
        
        try self.forEach { pointer, page in
            guard let document = try pointer.resolve() else {
                return false
            }
            
            for (key, value) in matching {
                guard document[key]?.makeBinary() ?? [] == value.makeBinary() else {
                    return true
                }
            }
            
            try page.remove(pointer)
            
            count += 1
            return true
        }
        
        return count
    }
    
    func forEach(closure: (DocumentReference, CollectionPage) throws -> (Bool)) throws {
        var iterating: CollectionPage = header
        
        var iterator = iterating.makeIterator()
        
        documents: while true {
            guard let documentPointer = iterator.next() else {
                guard let next = try iterating.nextPage?.resolve() as? CollectionPage else {
                    return
                }
                
                iterating = next
                iterator = iterating.makeIterator()
                continue documents
            }
            
            guard try closure(documentPointer, iterating) else {
                return
            }
        }
    }
    
    public func count() throws -> Int {
        var iterating: CollectionPage = header
        
        var iterator = iterating.makeIterator()
        var count = 0
        
        while true {
            guard iterator.next() != nil else {
                guard let next = try iterating.nextPage?.resolve() as? CollectionPage else {
                    return count
                }
                
                iterating = next
                iterator = iterating.makeIterator()
                continue
            }
            
            count += 1
        }
    }

    public func makeIterator() -> AnyIterator<Document> {
        var iterating: CollectionPage = header
        
        var iterator = iterating.makeIterator()
        
        return AnyIterator {
            do {
                while true {
                    guard let document = try iterator.next()?.resolve() else {
                        guard let next = try iterating.nextPage?.resolve() as? CollectionPage else {
                            return nil
                        }
                        
                        iterating = next
                        iterator = iterating.makeIterator()
                        continue
                    }
                    
                    return document
                }
            } catch {
                return nil
            }
        }
    }
}
