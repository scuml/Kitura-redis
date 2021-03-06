import SSLService
import Foundation
import Socket

// MARK: Redis with SSL

/// Extend Redis and RedisResp to connect using sslConfigs if SSLService is available.
/// Add https://github.com/Kitura/BlueSSLService to a project to enable.

extension Redis {


    /// Connects to a redis server
    ///
    /// - Parameter host: the server IP address.
    /// - Parameter port: port number.
    /// - Parameter sslConfig: Optional SSLService.Configuration.  eg:
    ///   SSL Configs can only use a password protected .p12 on Mac OS
    ///       let sslConfig = SSLService.Configuration(
    ///           withChainFilePath: "/keystore.p12",
    ///           withPassword: "pass",
    ///           usingSelfSignedCerts: true,
    ///           clientAllowsSelfSignedCertificates: true
    ///       )
    /// - Parameter callback: callback function for on completion, NSError will be nil if successful.
    public func connect(host: String, port: Int32, connectWithSSL: Bool = false, sslConfig: SSLService.Configuration? = nil, sslSkipVerification: Bool = false, callback: (NSError?) -> Void) {
        respHandle = RedisResp(host: host, port: port, connectWithSSL: connectWithSSL, sslConfig: sslConfig, sslSkipVerification: sslSkipVerification)

        if respHandle?.status == .notConnected {
            callback(createError("Failed to connect to Redis server", code: 2))
        } else {
            callback(nil)
        }
    }
}

extension RedisResp {

    convenience init(host: String, port: Int32, connectWithSSL: Bool = false, sslConfig: SSLService.Configuration? = nil, sslSkipVerification: Bool = false) {
        self.init()
        socket = try? Socket.create()
        if connectWithSSL{
            var sslConfigToUse = SSLService.Configuration()
            if let sslConfig = sslConfig{
                /// SSL Configs can only use a password protected .p12 on Mac OS
                // let sslConfig = SSLService.Configuration(
                //     withChainFilePath: "/keystore.p12",
                //     withPassword: "pass",
                //     usingSelfSignedCerts: true,
                //     clientAllowsSelfSignedCertificates: true
                // )
                sslConfigToUse = sslConfig
            }

            do{
                let sslService = try SSLService(usingConfiguration: sslConfigToUse)
                sslService?.skipVerification = sslSkipVerification
                socket?.delegate = sslService
            }catch{
                print(error)
            }
        }
        self.connect(host: host, port: port)
    }
}
