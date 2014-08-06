import six

if six.PY2:
    from deuce.drivers.swift import py2_swiftstoragedriver
    SwiftStorageDriver = py2_swiftstoragedriver.PY2_SwiftStorageDriver

else:
    from deuce.drivers.swift import swiftstoragedriver
    SwiftStorageDriver = swiftstoragedriver.SwiftStorageDriver

