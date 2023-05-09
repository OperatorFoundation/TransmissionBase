//
//  BaseConnection.swift
//  
//
//  Created by Dr. Brandon Wiley on 3/11/23.
//

import Foundation
#if os(macOS)
import os.log
#else
import Logging
#endif

import Chord
import Datable
import SwiftHexTools
import SwiftQueue
import TransmissionTypes
import Net

open class BaseConnection: Connection
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

    open func read(size: Int) -> Data?
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
                print("📻 TransmissionBase: requested read size was zero")
                return nil
            }

            if size <= buffer.count
            {
                let result = Data(buffer[0..<size])
                buffer = Data(buffer[size..<buffer.count])
                print("📻 TransmissionBase: TransmissionConnection.read(size: \(size)) -> returned \(result.count) bytes.")
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
            print("📻 TransmissionBase: TransmissionConnection.read(size: \(size)) -> returned \(result.count) bytes.")

            return result
        }
        catch
        {
            print("error in BaseConnection.read(\(size)): \(error)")
            return nil
        }
    }

    open func unsafeRead(size: Int) -> Data?
    {
        do
        {
            if size == 0
            {
                print("📻 TransmissionBase: requested read size was zero")
                return nil
            }

            if size <= buffer.count
            {
                let result = Data(buffer[0..<size])
                buffer = Data(buffer[size..<buffer.count])
                print("📻 TransmissionBase: TransmissionConnection.read(size: \(size)) -> returned \(result.count) bytes.")
                return result
            }

            let data = try networkRead(size: size)

            guard data.count > 0 else
            {
                print("📻 TransmissionBase: unsafeRead received 0 bytes from networkRead()")
                return nil
            }

            buffer.append(data)

            guard size <= buffer.count else
            {
                print("📻 TransmissionBase: unsafeRead requested size \(size) is larger than the buffer size \(buffer.count). Returning nil.")
                return nil
            }

            let result = Data(buffer[0..<size])
            buffer = Data(buffer[size..<buffer.count])
            print("📻 TransmissionBase: TransmissionConnection.read(size: \(size)) -> returned \(result.count) bytes.")

            return result
        }
        catch
        {
            print("📻 error in BaseConnection.unsafeRead(\(size)): \(error)")
            return nil
        }
    }

    open func read(maxSize: Int) -> Data?
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

            print("📻 TransmissionBase: TransmissionConnection.read(maxSize: \(maxSize)) - returned \(result.count) bytes")
            return result
        }
        else
        {
            // Buffer is empty, so we need to do a network read
            var data: Data?
            
            while self.buffer.count < maxSize
            {
                do
                {
                    data = try self.networkRead(size: maxSize - self.buffer.count)
                }
                catch
                {
                    print("📻 TransmissionBase: TransmissionConnection.read(maxSize: \(maxSize)) - Error trying to read from the network: \(error)")
                    break
                }

                guard let bytes = data else
                {
                    print("📻 TransmissionBase: TransmissionConnection.read(maxSize: \(maxSize)) - Error received a nil response when attempting to read from the network.")
                    break
                }

                guard bytes.count > 0 else
                {
                    print("📻 TransmissionBase: TransmissionConnection.read(maxSize: \(maxSize)) - Error received an empty response when attempting to read from the network.")
                    break
                }

                buffer.append(bytes)
            }
            
            let targetSize = min(maxSize, buffer.count)
            let result = Data(buffer[0..<targetSize])
            buffer = Data(buffer[targetSize..<buffer.count])
            
            print("📻 TransmissionBase: TransmissionConnection.read(maxSize: \(maxSize)) - returning \(result.count) bytes")
            return result
        }
    }


    open func write(string: String) -> Bool
    {
        let data = string.data
        let success = write(data: data)

        return success
    }

    open func write(data: Data) -> Bool
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
            print("📻 error in BaseConnection.write(\(data.count) bytes - \(data.hex) - \"\(data.string)\"")
            return false
        }

        return true
    }

    open func readWithLengthPrefix(prefixSizeInBits: Int) -> Data?
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

    open func writeWithLengthPrefix(data: Data, prefixSizeInBits: Int) -> Bool
    {
        return TransmissionTypes.writeWithLengthPrefix(data: data, prefixSizeInBits: prefixSizeInBits, connection: self)
    }

    open func identifier() -> Int
    {
        return self.id
    }

    open func close()
    {
        print("📻 BaseConnection close() called.")
    }

    open func networkWrite(data: Data) throws
    {
        throw BaseConnectionError.unimplemented
    }

    open func networkRead(size: Int, timeoutSeconds: Int = 10) throws -> Data
    {
        throw BaseConnectionError.unimplemented
    }
}

public enum BaseConnectionError: Error
{
    case unimplemented
}
