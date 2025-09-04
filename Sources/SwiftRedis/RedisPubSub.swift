/**
 * Copyright Echodot LLC 2022
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

import Foundation
import Network

/// Extend Redis by adding the Set operations
extension Redis {

    //
    //  MARK: Publish/Subscribe API functions
    //
    
    /// Publish a message to a channel
    ///
    /// - Parameter key: The name of the channel
    /// - Parameter message: The values to be added to the set.
    public func publish(_ channel: String, message: String) async -> (Int?, NSError?) {
        let response = await issueCommandInArray(["PUBLISH", channel, message])
        return self.redisIntegerResponseHandler(response)
    }


    
}

actor SubscriptionVars{
    var isListening: Bool = false
    var activeChannels: Set<String> = []
    var activePatterns: Set<String> = []

    func getIsListening() -> Bool{
        return isListening
    }
    func setIsListening(_ val: Bool){
        isListening = val
    }
    func addChannel(_ channel: String){
        self.activeChannels.insert(channel)
    }
    func removeChannel(_ channel: String){
        self.activeChannels.remove(channel)
    }
    func removeChannel(_ channel: Set<String>){
        self.activeChannels.subtract(channel)
    }
    func addPattern(_ pattern: String){
        self.activePatterns.insert(pattern)
    }
    func removePattern(_ pattern: String){
        self.activePatterns.remove(pattern)
    }
    func removePattern(_ pattern: Set<String>){
        self.activePatterns.subtract(pattern)
    }
    func getChannelsAndPatterns() -> (Set<String>, Set<String>){
        let channels = self.activeChannels
        let patterns = self.activePatterns
        return (channels, patterns)
    }
    func hasChannelsOrPatterns() -> Bool{
        return !self.activePatterns.isEmpty || !self.activeChannels.isEmpty
    }

}


///
/// A Redis Subscription must have its own socket to listen for incoming messages.
/// Create and assign a delegate to handle incoming messages
public class RedisSubscription: Redis{
    public var delegate: RedisSubscriptionDelegate?
    let subscribeQueue = DispatchQueue(label: "subscribe")
    
    var safevars: SubscriptionVars = SubscriptionVars()
    
    func listen() async{
        // Starts listener if activeChannels or activePatterns exist.
        // If not, stop the listener
        if await !safevars.hasChannelsOrPatterns(){
            print("no patterns")
            await safevars.setIsListening(false)
            return
        }
        if await safevars.getIsListening() == false{
            Task{
                await safevars.setIsListening(true)
                await checkMessages()

            }
        }
    }
    
    
    @discardableResult
    public func subscribe(channel: String) async ->  (Int?, NSError?) {
        await self.safevars.addChannel(channel)
        let response = await issueCommand("SUBSCRIBE", channel)
        await listen()
        return self.redisIntegerResponseHandler(response)
    }

    @discardableResult
    public func subscribe(pattern: String) async ->  (Int?, NSError?) {
        await self.safevars.addPattern(pattern)
        let response = await issueCommandInArray(["PSUBSCRIBE", pattern])
        await listen()
        return self.redisIntegerResponseHandler(response)
    }
    
    @discardableResult
    public func unsubscribe(channel: String) async -> Bool {
        await self.safevars.removeChannel(channel)
        let response = await issueCommandInArray(["UNSUBSCRIBE", channel])
        guard let responseArray = response.asArray, responseArray.count == 3, let responseType = responseArray[0].asString?.asString, let channel = responseArray[1].asString?.asString, let count = responseArray[2].asInteger else {print("unknown message response", response); return false}
        await listen()
        return true
    }
    @discardableResult
    public func unsubscribe(channel: Set<String>) async -> Bool {
        await self.safevars.removeChannel(channel)
        let response = await issueCommandInArray(["UNSUBSCRIBE"] + channel)
        guard let responseArray = response.asArray, responseArray.count == 3, let responseType = responseArray[0].asString?.asString, let channel = responseArray[1].asString?.asString, let count = responseArray[2].asInteger else {print("unknown message response", response); return false}
        await listen()
        return true
    }
    
    
    @discardableResult
    public func unsubscribe(pattern: String) async -> Bool {
        await self.safevars.removePattern(pattern)
        let response = await issueCommandInArray(["PUNSUBSCRIBE", pattern])
        guard let responseArray = response.asArray, responseArray.count == 3, let responseType = responseArray[0].asString?.asString, let channel = responseArray[1].asString?.asString, let count = responseArray[2].asInteger else {print("unknown message response", response); return false}
        await listen()
        return true
    }
    @discardableResult
    public func unsubscribe(pattern: Set<String>) async -> Bool {
        await self.safevars.removePattern(pattern)
        let response = await issueCommandInArray(["PUNSUBSCRIBE"] + pattern)
        guard let responseArray = response.asArray, responseArray.count == 3, let responseType = responseArray[0].asString?.asString, let channel = responseArray[1].asString?.asString, let count = responseArray[2].asInteger else {print("unknown message response", response); return false}
        await listen()
        return true
    }
    
    @discardableResult
    public func unsubscribeAll() async -> Bool {
        // if an active patterns or channels, unsubscribe to them and stop listening.
        // Return false if not listening
        let (channels, patterns) = await self.safevars.getChannelsAndPatterns()
        if channels.isEmpty && patterns.isEmpty{
            return false
        }
        if !channels.isEmpty{
            await self.unsubscribe(channel: channels)
        }
        if !patterns.isEmpty{
            await self.unsubscribe(pattern: patterns)
        }
        return true
    }
    
    
    func checkMessages() async{
        
        //let endTime = CFAbsoluteTimeGetCurrent() + 0.25  // let other threads act every x seconds
        print("checking messages...")
        while await safevars.getIsListening() {
            
            do{
                guard let responses = try await self.respHandle?.socketActor?.readNonBlocking() else {
                    try? await Task.sleep(nanoseconds: 100_000_000) // 100ms
                    continue
                }
                for response in responses{
                    
                    // Early exit if not an array
                    guard let responseArray = response.asArray,
                          !responseArray.isEmpty,
                          let responseType = responseArray[0].asString?.asString else {
                        print("unknown message response", response)
                        continue
                    }
                    
                    // Process based on message type
                    switch responseType {
                    case "message" where responseArray.count == 3:
                        if let channel = responseArray[1].asString?.asString,
                           let message = responseArray[2].asString?.asString {
                            delegate?.onMessage(channel, message)
                        } else {
                            print("malformed message response", response)
                        }
                        
                    case "pmessage" where responseArray.count == 4:
                        if let pattern = responseArray[1].asString?.asString,
                           let channel = responseArray[2].asString?.asString,
                           let message = responseArray[3].asString?.asStringSafe {
                            delegate?.onMessage(pattern, channel, message)
                        } else {
                            print("malformed pmessage response", response)
                        }
                        
                    default:
                        print("unknown message type: \(responseType), array count: \(responseArray.count)")
                    }
                }
            }catch {
                print("Error reading: \(error)")
                break
            }
        }
    }
    
}

public protocol RedisSubscriptionDelegate{
    func onMessage(_ channel: String, _ message: String)
    func onMessage(_ pattern: String, _ channel: String, _ message: String)

}
