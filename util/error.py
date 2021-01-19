class BaseError(Exception):
    '''Root of My Error'''
    pass

class LocalError(BaseError):
    '''Internal error'''
    pass

class RemoteError(BaseError):
    '''Remote Error includes network erros and remote host errors'''
    pass

class NetworkError(RemoteError):
    '''LAN or WAN error'''
    pass

class RemoteHostError(RemoteError):
    '''remote host error'''
    pass

class DecodeError(RemoteHostError):
    '''Cannot decode correctly the data from remote host'''
    pass

class FormatError(RemoteHostError):
    '''The data from remote host is decoded but with wrong format'''
    pass

class NoDataError(RemoteHostError):
    '''Cannot find useful data'''
    pass


