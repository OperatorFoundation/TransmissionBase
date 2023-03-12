//
//  BaseConnection.swift
//  
//
//  Created by Dr. Brandon Wiley on 3/11/23.
//

import Foundation

import Chord
import Datable
import Logging
import SwiftHexTools
import SwiftQueue
import TransmissionTypes
import Net

public class BaseConnection: Connection
{
    let id: Int
    let log: Logger?

    var udpIncomingPort: Int? = nil

    var connectLock = DispatchSemaphore(value: 1)
    var readLock = DispatchSemaphore(value: 1)
    var writeLock = DispatchSemaphore(value: 1)

    var buffer: Data = Data()

    public init?(id: Int, logger: Logger? = nil)
    {
        self.id = id
        self.log = logger
    }

    public func read(size: Int) -> Data?
    {
        defer
        {
            readLock.signal()
        }
        readLock.wait()

        do
        {
            if size == 0
            {
                print("TransmissionLinux: requested read size was zero")
                return nil
            }

            if size <= buffer.count
            {
                let result = Data(buffer[0..<size])
                buffer = Data(buffer[size..<buffer.count])
                print("TransmissionLinux: TransmissionConnection.read(size: \(size)) -> returned \(result.count) bytes.")
                return result
            }

            let data = try networkRead(size: size)

            guard data.count > 0 else
            {
                return nil
            }

            buffer.append(data)

            guard size <= buffer.count else
            {
                return nil
            }

            let result = Data(buffer[0..<size])
            buffer = Data(buffer[size..<buffer.count])
            print("TransmissionLinux: TransmissionConnection.read(size: \(size)) -> returned \(result.count) bytes.")

            return result
        }
        catch
        {
            print("error in BaseConnection.read(\(size)): \(error)")
            return nil
        }
    }

    public func unsafeRead(size: Int) -> Data?
    {
        do
        {
            print("TransmissionLinux: unsafeRead(size: \(size))")

            if size == 0
            {
                print("TransmissionLinux: requested read size was zero")
                return nil
            }

            if size <= buffer.count
            {
                let result = Data(buffer[0..<size])
                buffer = Data(buffer[size..<buffer.count])
                print("TransmissionLinux: TransmissionConnection.read(size: \(size)) -> returned \(result.count) bytes.")
                return result
            }

            print("TransmissionLinux: unsafeRead calling networkRead()")
            let data = try networkRead(size: size)
            print("TransmissionLinux: unsafeRead returned from networkRead()")

            guard data.count > 0 else
            {
                print("TransmissionLinux: unsafeRead received 0 bytes from networkRead()")
                return nil
            }

            buffer.append(data)

            guard size <= buffer.count else
            {
                print("TransmissionLinux: unsafeRead requested size \(size) is larger than the buffer size \(buffer.count). Returning nil.")
                return nil
            }

            let result = Data(buffer[0..<size])
            buffer = Data(buffer[size..<buffer.count])
            print("TransmissionLinux: TransmissionConnection.read(size: \(size)) -> returned \(result.count) bytes.")

            return result
        }
        catch
        {
            print("error in BaseConnection.unsafeRead(\(size)): \(error)")
            return nil
        }
    }

    public func read(maxSize: Int) -> Data?
    {
        defer
        {
            readLock.signal()
        }
        readLock.wait()

        if maxSize == 0
        {
            return nil
        }

        let size = maxSize <= buffer.count ? maxSize : buffer.count

        if size > 0
        {
            let result = Data(buffer[0..<size])
            buffer = Data(buffer[size..<buffer.count])

            print("TransmissionLinux: TransmissionConnection.read(maxSize: \(maxSize)) - returned \(result.count) bytes")
            return result
        }
        else
        {
            // Buffer is empty, so we need to do a network read
            var data: Data?

            do
            {
                let data = try self.networkRead(size: maxSize - self.buffer.count)
            }
            catch
            {
                print("TransmissionLinux: TransmissionConnection.read(maxSize: \(maxSize)) - Error trying to read from the network: \(error)")
                return nil
            }

            guard let bytes = data else
            {
                print("TransmissionLinux: TransmissionConnection.read(maxSize: \(maxSize)) - Error received a nil response when attempting to read from the network.")
                return nil
            }

            guard bytes.count > 0 else
            {
                print("TransmissionLinux: TransmissionConnection.read(maxSize: \(maxSize)) - Error received an empty response when attempting to read from the network.")
                return nil
            }

            buffer.append(bytes)
            let targetSize = min(maxSize, buffer.count)
            let result = Data(buffer[0..<targetSize])
            buffer = Data(buffer[targetSize..<buffer.count])

            print("TransmissionLinux: TransmissionConnection.read(maxSize: \(maxSize)) - returned \(result.count) bytes")
            return result
        }
    }


    public func write(string: String) -> Bool
    {
        let data = string.data
        let success = write(data: data)

        print("TransmissionLinux: TransmissionConnection.networkWrite -> write(string:), success: \(success)")
        return success
    }

    public func write(data: Data) -> Bool
    {
        defer
        {
            self.writeLock.signal()
        }
        self.writeLock.wait()

        do
        {
            try networkWrite(data: data)
        }
        catch
        {
            print("error in BaseConnection.write(\(data.count) bytes - \(data.hex) - \"\(data.string)\"")
            return false
        }

        return true
    }

    public func readWithLengthPrefix(prefixSizeInBits: Int) -> Data?
    {
        defer
        {
            self.readLock.signal()
        }
        self.readLock.wait()

        guard let result = TransmissionTypes.readWithLengthPrefix(prefixSizeInBits: prefixSizeInBits, connection: self) else
        {
            return nil
        }

        return result
    }

    public func writeWithLengthPrefix(data: Data, prefixSizeInBits: Int) -> Bool
    {
        defer
        {
            self.writeLock.signal()
        }
        self.writeLock.wait()

        return TransmissionTypes.writeWithLengthPrefix(data: data, prefixSizeInBits: prefixSizeInBits, connection: self)
    }

    public func identifier() -> Int
    {
        return self.id
    }

    public func close()
    {
    }

    private func networkWrite(data: Data) throws
    {
        throw BaseConnectionError.unimplemented
    }

    private func networkRead(size: Int) throws -> Data
    {
        throw BaseConnectionError.unimplemented
    }
}

public enum BaseConnectionError: Error
{
    case unimplemented
}
