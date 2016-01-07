//
//  RedisMulti.swift
//  SwiftRedis
//
//  Created by Samuel Kallner on 05/01/2016.
//  Copyright © 2016 Daniel Firsht. All rights reserved.
//

import Foundation

public class RedisMulti {
    let redis: Redis
    var queuedCommands = [[RedisString]]()
    
    init(redis: Redis) {
        self.redis = redis
    }
    
    // ************************
    //  Commands to be Queued *
    // ************************
    
    
    public func append(key: String, value: String) -> RedisMulti {
        queuedCommands.append([RedisString("APPEND"), RedisString(key), RedisString(value)])
        return self
    }
    
    public func bitcount(key: String) -> RedisMulti {
        queuedCommands.append([RedisString("BITCOUNT"), RedisString(key)])
        return self
    }
    
    public func bitcount(key: String, start: Int, end: Int) -> RedisMulti {
        queuedCommands.append([RedisString("BITCOUNT"), RedisString(key), RedisString(start), RedisString(end)])
        return self
    }
    
    public func bitop(destKey: String, and: String...) -> RedisMulti {
        var command = [RedisString("BITOP"), RedisString("AND"), RedisString(destKey)]
        for key in and {
            command.append(RedisString(key))
        }
        queuedCommands.append(command)
        return self
    }
    
    public func bitop(destKey: String, not: String) -> RedisMulti {
        queuedCommands.append([RedisString("BITOP"), RedisString("NOT"), RedisString(destKey), RedisString(not)])
        return self
    }
    
    public func bitop(destKey: String, or: String...) -> RedisMulti {
        var command = [RedisString("BITOP"), RedisString("OR"), RedisString(destKey)]
        for key in or {
            command.append(RedisString(key))
        }
        queuedCommands.append(command)
        return self
    }
    
    public func bitop(destKey: String, xor: String...) -> RedisMulti {
        var command = [RedisString("BITOP"), RedisString("XOR"), RedisString(destKey)]
        for key in xor {
            command.append(RedisString(key))
        }
        queuedCommands.append(command)
        return self
    }
    
    public func bitpos(key: String, bit:Bool) -> RedisMulti {
        queuedCommands.append([RedisString("BITPOS"), RedisString(key), RedisString(bit ? "1" : "0")])
        return self
    }
    
    public func bitpos(key: String, bit:Bool, start: Int) -> RedisMulti {
        queuedCommands.append([RedisString("BITPOS"), RedisString(key), RedisString(bit ? "1" : "0"), RedisString(start)])
        return self
    }
    
    public func bitpos(key: String, bit:Bool, start: Int, end: Int) -> RedisMulti {
        queuedCommands.append([RedisString("BITPOS"), RedisString(key), RedisString(bit ? "1" : "0"), RedisString(start), RedisString(end)])
        return self
    }
    
    public func decr(key: String, by: Int=1) -> RedisMulti {
        queuedCommands.append([RedisString("DECRBY"), RedisString(key), RedisString(by)])
        return self
    }
    
    public func del(keys: String...) -> RedisMulti {
        var command = [RedisString("DEL")]
        for key in keys {
            command.append(RedisString(key))
        }
        queuedCommands.append(command)
        return self
    }
    
    public func get(key: String) -> RedisMulti {
        queuedCommands.append([RedisString("GET"), RedisString(key)])
        return self
    }
    
    public func getbit(key: String, offset: Int) -> RedisMulti {
        queuedCommands.append([RedisString("GETBIT"), RedisString(key), RedisString(offset)])
        return self
    }
    
    public func getrange(key: String, start: Int, end: Int) -> RedisMulti {
        queuedCommands.append([RedisString("GETRANGE"), RedisString(key), RedisString(start), RedisString(end)])
        return self
    }
    
    public func getSet(key: String, value: String) -> RedisMulti {
        queuedCommands.append([RedisString("GETSET"), RedisString(key), RedisString(value)])
        return self
    }
    
    public func getSet(key: String, value: RedisString) -> RedisMulti {
        queuedCommands.append([RedisString("GETSET"), RedisString(key), value])
        return self
    }
    
    public func incr(key: String, by: Int=1) -> RedisMulti {
        queuedCommands.append([RedisString("INCRBY"), RedisString(key), RedisString(by)])
        return self
    }
    
    public func incr(key: String, byFloat: Float) -> RedisMulti {
        queuedCommands.append([RedisString("INCRBYFLOAT"), RedisString(key), RedisString(Double(byFloat))])
        return self
    }
    
    public func mget(keys: String...) -> RedisMulti {
        var command = [RedisString("MGET")]
        for key in keys {
            command.append(RedisString(key))
        }
        queuedCommands.append(command)
        return self
    }
    
    public func mset(keyValuePairs: (String, String)..., exists: Bool=true) -> RedisMulti {
        return msetArrayOfKeyValues(keyValuePairs, exists: exists)
    }
    
    public func msetArrayOfKeyValues(keyValuePairs: [(String, String)], exists: Bool=true) -> RedisMulti {
        var command = [RedisString(exists ? "MSET" : "MSETNX")]
        for (key, value) in keyValuePairs {
            command.append(RedisString(key))
            command.append(RedisString(value))
        }
        queuedCommands.append(command)
        return self
    }
    
    public func mset(keyValuePairs: (String, RedisString)..., exists: Bool=true) -> RedisMulti {
        return msetArrayOfKeyValues(keyValuePairs, exists: exists)
    }
    
    public func msetArrayOfKeyValues(keyValuePairs: [(String, RedisString)], exists: Bool=true) -> RedisMulti {
        var command = [RedisString(exists ? "MSET" : "MSETNX")]
        for (key, value) in keyValuePairs {
            command.append(RedisString(key))
            command.append(value)
        }
        queuedCommands.append(command)
        return self
    }
    
    public func select(db: Int) -> RedisMulti {
        queuedCommands.append([RedisString("SELECT"), RedisString(db)])
        return self
    }
    
    public func set(key: String, value: String, exists: Bool?=nil, expiresIn: NSTimeInterval?=nil) -> RedisMulti {
        var command = [RedisString("SET"), RedisString(key), RedisString(value)]
        if  let exists = exists  {
            command.append(RedisString(exists ? "XX" : "NX"))
        }
        if  let expiresIn = expiresIn  {
            command.append(RedisString("PX"))
            command.append(RedisString(Int(expiresIn * 1000.0)))
        }
        queuedCommands.append(command)
        return self
    }
    
    public func set(key: String, value: RedisString, exists: Bool?=nil, expiresIn: NSTimeInterval?=nil) -> RedisMulti {
        var command = [RedisString("SET"), RedisString(key), value]
        if  let exists = exists  {
            command.append(RedisString(exists ? "XX" : "NX"))
        }
        if  let expiresIn = expiresIn  {
            command.append(RedisString("PX"))
            command.append(RedisString(Int(expiresIn * 1000.0)))
        }
        queuedCommands.append(command)
        return self
    }
    
    public func setbit(key: String, offset: Int, value: Bool) -> RedisMulti {
        queuedCommands.append([RedisString("SETBIT"), RedisString(key), RedisString(offset), RedisString(value ? "1" : "0")])
        return self
    }
    
    public func setrange(key: String, offset: Int, value: String) -> RedisMulti {
        queuedCommands.append([RedisString("SETRANGE"), RedisString(key), RedisString(offset), RedisString(value)])
        return self
    }
    
    public func strlen(key: String) -> RedisMulti {
        queuedCommands.append([RedisString("STRLEN"), RedisString(key)])
        return self
    }
    
    
    // **********************
    //  Run the transaction *
    // **********************
    
    public func exec(callback: (RedisResponse) -> Void) {
        redis.issueCommand("MULTI") {(multiResponse: RedisResponse) in
            switch(multiResponse) {
                case .Status(let status):
                    if  status == "OK"  {
                        var idx = -1
                        var handler: ((RedisResponse) -> Void)? = nil
                        
                        let actualHandler = {(response: RedisResponse) in
                            switch(response) {
                                case .Status(let status):
                                    if  status == "QUEUED"  {
                                        idx++
                                        if  idx < self.queuedCommands.count  {
                                            // Queue another command to Redis
                                            self.redis.issueCommandInArray(self.queuedCommands[idx], callback: handler!)
                                        }
                                        else {
                                            self.redis.issueCommand("EXEC", callback: callback)
                                        }
                                    }
                                    else {
                                        self.execQueueingFailed(response, callback: callback)
                                    }
                                default:
                                    self.execQueueingFailed(response, callback: callback)
                            }
                        }
                        handler = actualHandler
                        
                        actualHandler(RedisResponse.Status("QUEUED"))
                    }
                    else {
                        callback(multiResponse)
                    }
                default:
                    callback(multiResponse)
            }
        }
    }
    
    private func execQueueingFailed(response: RedisResponse, callback: (RedisResponse) -> Void) {
        redis.issueCommand("DISCARD") {(_: RedisResponse) in
            callback(response)
        }
    }
}