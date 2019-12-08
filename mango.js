var async = require('async')
var pull = require('pull-stream')
var multicb = require('multicb')
var crypto = require('crypto')
var IPFS = require('ipfs')
var debug = require('debug')('mango')

var ipfs;
if (process.env['IPFS_PATH'] !== "") {
    ipfs = new IPFS(process.env['IPFS_PATH'])
} else {
    ipfs = new IPFS()
}
var Web3 = require('web3')
var rlp = require('rlp')
var ethUtil = require('ethereumjs-util')
var snapshot = require('./snapshot.js')
var repoABI = require('./MangoRepoABI.json')

// from https://github.com/clehner/memory-pull-git-repo
function gitHash (obj, data) {
  var hasher = crypto.createHash('sha1')
  hasher.update(obj.type + ' ' + obj.length + '\0')
  hasher.update(data)
  return hasher.digest('hex')
}

// FIXME: move into context?
function ipfsPut (buf, enc, cb) {
    debug('-- IPFS PUT')
  ipfs.object.put(buf, { enc }, function (err, node) {
    if (err) {
      return cb(err)
    }
    debug('  hash', node.toJSON().multihash)
    cb(null, node.toJSON().multihash)
  })
}

// FIXME: move into context?
function ipfsGet (key, cb) {
  debug('-- IPFS GET', key)
  ipfs.object.get(key, { enc: 'base58' }, function (err, node) {
    if (err) {
      return cb(err)
    }
    cb(null, node.toJSON().data)
  })
}



class Repo {

constructor(address, user) {
  debug('LOADING REPO', address)

  this.web3 = new Web3(new Web3.providers.HttpProvider(process.env['ETHEREUM_RPC_URL'] || 'http://localhost:8545'))
  try {
    this.web3.eth.defaultAccount = user || this.web3.eth.coinbase
  } catch (e) {
  }

  this.repoContract = this.web3.eth.contract(repoABI).at(address)
}

_loadObjectMap(cb) {
  debug('LOADING OBJECT MAP')
  var self = this
  self._objectMap = {}
  self.snapshotGetAll(function (err, res) {
    if (err) return cb(err)

    async.each(res, function (item, cb) {
      ipfsGet(item, function (err, data) {
        if (err) return cb(err)
        Object.assign(self._objectMap, snapshot.parse(data))
        cb()
      })
    }, function (err) {
      cb(err)
    })
  })
}

_ensureObjectMap(cb) {
  if (this._objectMap === undefined) {
    this._loadObjectMap(cb)
  } else {
    cb()
  }
}

snapshotAdd(hash, cb) {
  debug('SNAPSHOT ADD', hash)
  this.repoContract.addSnapshot(hash, {gas: 1000000}, cb)
}

snapshotGetAll(cb) {
  debug('SNAPSHOT GET ALL')
  var count = this.repoContract.snapshotCount().toNumber()
  var snapshots = []
  debug('  REFCOUNT=', count)
  for (var i = 0; i < count; i++) {
    snapshots.push(this.repoContract.getSnapshot(i))
  }

  cb(null, snapshots)
}

contractGetRef() {
  debug('REF GET', ...arguments)
  return this.repoContract.getRef(...arguments)
}

contractSetRef() {
  debug('REF SET', ...arguments)
  return this.repoContract.setRef(...arguments)
}

// FIXME: should be fully asynchronous
contractAllRefs(cb) {
  var refcount = this.repoContract.refCount().toNumber()
  debug('REFCOUNT', refcount)

  var refs = {}
  for (var i = 0; i < refcount; i++) {
    var key = this.repoContract.refName(i)
    refs[key] = this.contractGetRef(key)
    debug('REF GET', i, key, refs[key])
  }

  cb(null, refs)
}

refs(prefix) {
  var refcount = this.repoContract.refCount().toNumber()
  debug('REFCOUNT', refcount)

  var refs = {}
  for (var i = 0; i < refcount; i++) {
    var key = this.repoContract.refName(i)
    refs[key] = this.contractGetRef(key)
    debug('REF GET', i, key, refs[key])
  }

  var refNames = Object.keys(refs)
  i = 0
  return function (abort, cb) {
    if (abort) return
    if (i >= refNames.length) return cb(true)
    var refName = refNames[i++]
    cb(null, {
      name: refName,
      hash: refs[refName]
    })
  }
}

// FIXME: this is hardcoded for HEAD -> master
symrefs(a) {
  var i = 0
  return function (abort, cb) {
    if (abort) return
    if (i > 0) return cb(true)
    i++
    cb(null, {
      name: 'HEAD',
      ref: 'refs/heads/master'
    })
  }
}

hasObject(hash, cb) {
  var self = this

  debug('HAS OBJ', hash)

  this._ensureObjectMap(function () {
    debug('HAS OBJ', hash in self._objectMap)
    cb(null, hash in self._objectMap)
  })
}

getObject(hash, cb) {
  var self = this

  debug('GET OBJ', hash)

  this._ensureObjectMap(function (err) {
    if (err) return cb(err)

    if (!self._objectMap[hash]) {
      return cb('Object not present with key ' + hash)
    }

    ipfsGet(self._objectMap[hash], function (err, data) {
      if (err) return cb(err)

      var res = rlp.decode(data)

      return cb(null, {
        type: res[0].toString(),
        length: parseInt(res[1].toString(), 10),
        read: pull.once(res[2])
      })
    })
  })
}

update(readRefUpdates, readObjects, cb) {
  debug('UPDATE')

  var done = multicb({pluck: 1})
  var self = this

  if (readObjects) {
    var doneReadingObjects = function () {
      ipfsPut(snapshot.create(self._objectMap), null, function (err, ipfsHash) {
        if (err) {
          return done(err)
        }

        self.snapshotAdd(ipfsHash, function () {
          done()
        })
      })
    }

    // FIXME
    self._objectMap = self._objectMap || {}

    readObjects(null, function next (end, object) {
      if (end) {
        return doneReadingObjects(end === true ? null : end)
      }

      pull(
        object.read,
        pull.collect(function (err, bufs) {
          if (err) {
            return doneReadingObjects(err)
          }

          var buf = Buffer.concat(bufs)
          var hash = gitHash(object, buf)

          debug('UPDATE OBJ', hash, object.type, object.length)

          var data = rlp.encode([ ethUtil.toBuffer(object.type), ethUtil.toBuffer(object.length.toString()), buf ])

          ipfsPut(data, null, function (err, ipfsHash) {
            if (err) {
              return doneReadingObjects(err)
            }

            self._objectMap[hash] = ipfsHash
            readObjects(null, next)
          })
        })
      )
    })
  }

  if (readRefUpdates) {
    var doneReadingRefs = done()

    readRefUpdates(null, function next (end, update) {
      if (end) {
        return doneReadingRefs(end === true ? null : end)
      }

      debug('UPDATE REF', update.name, update.new, update.old)

      // FIXME: make this async
      var ref = self.contractGetRef(update.name)
      if (typeof(ref) === 'string' && ref.length === 0) {
        ref = null
      }

      if (update.old !== ref) {
        return doneReadingRefs(new Error(
          'Ref update old value is incorrect. ' +
          'ref: ' + update.name + ', ' +
          'old in update: ' + update.old + ', ' +
          'old in repo: ' + ref
        ))
      }

      if (update.new) {
        // FIXME: make this async
          self.contractSetRef(update.name, update.new, {gas: 1000000})
      } else {
        // FIXME: make this async
        self.repoContract.deleteRef(update.name)
      }

      readRefUpdates(null, next)
    })
  }

  done(function (err) {
    if (err) {
      return cb(err)
    }
    cb()
  })
}
}

module.exports = Repo
