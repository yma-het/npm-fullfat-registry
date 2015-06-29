var follow = require('follow')
var fs = require('fs')
var EE = require('events').EventEmitter
var util = require('util')
var url = require('url')
var path = require('path')
var tmp = path.resolve(__dirname, 'tmp')
var mkdirp = require('mkdirp')
var rimraf = require('rimraf')
var assert = require('assert')
var stream = require('stream')
var util = require('util')
var crypto = require('crypto')
var once = require('once')
var parse = require('parse-json-response')
var hh = require('http-https')
var crypto = require('crypto');

var debug = require('debug')
function formatArgs() {
  var args = arguments;
  var useColors = this.useColors;
  var name = this.namespace;

  args[0] = new Date().toUTCString()
    +' +' + debug.humanize(this.diff) + 
    + ' ' + name + ' ' + args[0];

  return args;
}
debug.formatArgs = formatArgs

function randomValueHex (len) {
    return crypto.randomBytes(Math.ceil(len/2))
        .toString('hex') // convert to hexadecimal format
        .slice(0,len);   // return required number of characters
}



var slice = [].slice

var getLogger = function(name){
  return debug('replicate:'+name)
}

var version = require('./package.json').version
var ua = 'npm FullFat/' + version + ' node/' + process.version
var readmeTrim = require('npm-registry-readme-trim')

util.inherits(FullFat, EE)

module.exports = FullFat

dbg = undefined

function FullFat(conf) {
  if (!conf.skim || !conf.fat) {
    throw new Error('skim and fat database urls required')
  }

  this.skim = url.parse(conf.skim).href
  this.skim = this.skim.replace(/\/+$/, '')

  var f = url.parse(conf.fat)
  this.fat = f.href
  this.fat = this.fat.replace(/\/+$/, '')
  delete f.auth
  this.publicFat = url.format(f)
  this.publicFat = this.publicFat.replace(/\/+$/, '')

  this.registry = null
  if (conf.registry) {
    this.registry = url.parse(conf.registry).href
    this.registry = this.registry.replace(/\/+$/, '')
  }

  this.ua = conf.ua || ua
  this.inactivity_ms = conf.inactivity_ms || 1000 * 60 * 60
  this.seqFile = conf.seq_file
  this.writingSeq = false
  this.error = false
  this.since = 0
  this.follow = null

  // set to true to log missing attachments only.
  // otherwise, emits an error.
  this.missingLog = conf.missing_log || false

  this.whitelist = conf.whitelist || [ /.*/ ]

  this.tmp = conf.tmp
  if (!this.tmp) {
    var rand = crypto.randomBytes(6).toString('hex')
    this.tmp = path.resolve('npm-fullfat-tmp-' + process.pid + '-' + rand)
  }

  this.boundary = 'npmFullFat-' + crypto.randomBytes(6).toString('base64')

  this.readSeq(this.seqFile)
}

FullFat.prototype.readSeq = function(file) {
  if (!this.seqFile)
    process.nextTick(this.start.bind(this))
  else
    fs.readFile(file, 'ascii', this.gotSeq.bind(this))
}

FullFat.prototype.gotSeq = function(er, data) {
  if (er && er.code === 'ENOENT')
    data = '0'
  else if (er)
    return this.emit('error', er)

  data = +data || 0
  this.since = data
  this.start()
}

FullFat.prototype.start = function() {
  if (this.follow)
    return this.emit('error', new Error('already started'))

  this.emit('start')
  this.follow = follow({
    db: this.skim,
    since: this.since,
    inactivity_ms: this.inactivity_ms
  }, this.onchange.bind(this))
  this.follow.on('error', this.emit.bind(this, 'error'))
}

FullFat.prototype._emit = function(ev, arg) {
  // Don't emit errors while writing seq
  if (ev === 'error' && this.writingSeq) {
    this.error = arg
  } else {
    EventEmitter.prototype.emit.apply(this, arguments)
  }
}

FullFat.prototype.writeSeq = function() {
  var seq = +this.since
  if (this.seqFile && !this.writingSeq && seq > 0) {
    var data = seq + '\n'
    var file = this.seqFile + '.' + seq
    this.writingSeq = true
    fs.writeFile(file, data, 'ascii', function(writeEr) {
      var er = this.error
      if (er)
        this.emit('error', er)
      else if (!writeEr) {
        fs.rename(file, this.seqFile, function(mvEr) {
          this.writingSeq = false
          var er = this.error
          if (er)
            this.emit('error', er)
          else if (!mvEr)
            this.emit('sequence', seq)
        }.bind(this))
      }
    }.bind(this))
  }
}

FullFat.prototype.onchange = function(er, change) {
  if (er)
    return this.emit('error', er)

  if (!change.id)
    return

  this.pause()
  this.since = change.seq

  change.log = getLogger(change.id)

  this.emit('change', change)

  if (change.deleted)
    this.delete(change)
  else
    this.getDoc(change)
}

FullFat.prototype.retryReq = function retryReq(req, fallback) {
  var self = this

  req.setTimeout(this.inactivity_ms, function() {
    self.emit('retry', req.path)
    req.abort()
    fallback()
  })
}

FullFat.prototype.getDoc = function(change) {
  var q = '?revs=true&att_encoding_info=true'
  var opt = url.parse(this.skim + '/' + change.id + q)
  opt.method = 'GET'
  opt.headers = {
    'user-agent': this.ua,
    agent: false
  }

  change.log('getDoc')
  
  var req = hh.get(opt)
  req.on('error', this.emit.bind(this, 'error'))
  req.on('response', parse(this.ongetdoc.bind(this, change)))

  this.retryReq(req, this.getDoc.bind(this, change))
}

FullFat.prototype.ongetdoc = function(change, er, data, res) {
  
  change.log('ongetdoc')
  
  if (er)
    this.emit('error', er)
  else {
    change.doc = data
    if (change.id.match(/^_design\//))
      this.putDesign(change)
    else if (data.time && data.time.unpublished)
      this.unpublish(change)
    else
      this.putDoc(change)
  }
}

FullFat.prototype.onFetchRev = function onFetchRev(change, er, f, res) {
  change.log(arguments.callee.name)
  //console.log('DELETING, f = ' + change.fat)
  change.fat = change.doc
  if (f._rev == undefined){
    change.fat._rev = '1-' + randomValueHex(32)
  } else{
    change.fat._rev = f._rev
  }
  //change.fat._rev = f._rev
  this.put(change, [])

}


FullFat.prototype.unpublish = function unpublish(change) {
  change.log(arguments.callee.name)
  
  //console.log('DELETING, f = ' + change.fat)
  //change.fat = change.doc


  var opt = url.parse(this.fat + '/' + change.id)

  opt.method = 'GET'
  opt.headers = {
    'user-agent': this.ua,
    agent: false
  }
  var req = hh.get(opt)
  req.on('error', this.emit.bind(this, 'error'))
  req.on('response', parse(this.onFetchRev.bind(this, change)))

  this.retryReq(req, this.unpublish.bind(this, change))




 // this.put(change, [])
}

FullFat.prototype.putDoc = function putDoc(change) {
  change.log(arguments.callee.name)

  var q = '?revs=true&att_encoding_info=true'
  var opt = url.parse(this.fat + '/' + change.id + q)

  opt.method = 'GET'
  opt.headers = {
    'user-agent': this.ua,
    agent: false
  }
  var req = hh.get(opt)
  req.on('error', this.emit.bind(this, 'error'))
  req.on('response', parse(this.onfatget.bind(this, change)))

  this.retryReq(req, this.putDoc.bind(this, change))
}

FullFat.prototype.putDesign = function putDesign(change) {
  change.log(arguments.callee.name)
  var doc = change.doc
  this.pause()
  var opt = url.parse(this.fat + '/' + change.id + '?new_edits=false')
  var b = new Buffer(JSON.stringify(doc), 'utf8')
  opt.method = 'PUT'
  opt.headers = {
    'user-agent': this.ua,
    'content-type': 'application/json',
    'content-length': b.length,
    agent: false
  }

  var req = hh.request(opt)
  req.on('response', parse(this.onputdesign.bind(this, change)))
  req.on('error', this.emit.bind(this, 'error'))
  req.end(b)

  this.retryReq(req, this.putDesign.bind(this, change))
}

FullFat.prototype.onputdesign = function onputdesign(change, er, data, res) {
  change.log(arguments.callee.name)
  if (er)
    return this.emit('error', er)
  this.emit('putDesign', change, data)
  this.resume()
}

FullFat.prototype.delete = function delete_(change) {
  change.log(arguments.callee.name)
  var name = change.id

  var opt = url.parse(this.fat + '/' + name)
  opt.headers = {
    'user-agent': this.ua,
    agent: false
  }
  opt.method = 'HEAD'

  var req = hh.request(opt)
  req.on('response', this.ondeletehead.bind(this, change))
  req.on('error', this.emit.bind(this, 'error'))
  req.end()

  this.retryReq(req, this.delete.bind(this, change))
}

FullFat.prototype.ondeletehead = function ondeletehead(change, res) {
  change.log(arguments.callee.name)
  // already gone?  totally fine.  move on, nothing to delete here.
  if (res.statusCode === 404)
    return this.afterDelete(change)

  var rev = res.headers.etag.replace(/^"|"$/g, '')
  opt = url.parse(this.fat + '/' + change.id + '?rev=' + rev)
  opt.headers = {
    'user-agent': this.ua,
    agent: false
  }
  opt.method = 'DELETE'
  var req = hh.request(opt)
  req.on('response', parse(this.ondelete.bind(this, change)))
  req.on('error', this.emit.bind(this, 'error'))
  req.end()

  this.retryReq(req, this.ondeletehead.bind(this, change, res))
}

FullFat.prototype.ondelete = function ondelete(change, er, data, res) {
  change.log(arguments.callee.name)
  if (er && er.statusCode === 404)
    this.afterDelete(change)
  else if (er)
    this.emit('error', er)
  else
    // scorch the earth! remove fully! repeat until 404!
    this.delete(change)
}

FullFat.prototype.afterDelete = function afterDelete(change) {
  change.log(arguments.callee.name)
  this.emit('delete', change)
  this.resume()
}

FullFat.prototype.onfatget = function onfatget(change, er, f, res) {
  change.log(arguments.callee.name)
  if (er && er.statusCode !== 404)
    return this.emit('error', er)

  if (er) {
    f = JSON.parse(JSON.stringify(change.doc))
    f._attachments = {}
  }
  //#anchor
  f._attachments = {}
  //if (f._rev == undefined){
  //  f._rev = '1-' + randomValueHex(32)
  //} else {
  //  console.log("!!!not modofoed!!!")
  //}
  change.fat = f
  this.merge(change)
}


FullFat.prototype.merge = function merge(change) {
  change.log(arguments.callee.name)
  
  var s = change.doc
  var f = change.fat
  var fat_revision = f._rev

  dbg && change.log(arguments.callee.name, 'source', s)
  dbg && change.log(arguments.callee.name, 'fat', f)

  // if no versions in the skim record, then nothing to fetch
  if (!s.versions){
    change.log('no versions')
    return this.resume()
  }

  // Only fetch attachments if it's on the list.
  var pass = true
  if (this.whitelist.length) {
    change.log('processing whitelist')
    pass = false
    for (var i = 0; !pass && i < this.whitelist.length; i++) {
      var w = this.whitelist[i]
      if (typeof w === 'string')
        pass = w === change.id
      else
        pass = w.exec(change.id)
    }
    if (!pass) {
      f._attachments = {}
      return this.fetchAll(change, [], [])
    }
  }

  var need = []
  var changed = false
  for (var v in s.versions) {
    var tgz = s.versions[v].dist.tarball
    var att = path.basename(url.parse(tgz).pathname)
    var ver = s.versions[v]
    f.versions = f.versions || {}

    if (!f.versions[v] || f.versions[v].dist.shasum !== ver.dist.shasum) {
      f.versions[v] = s.versions[v]
      need.push(v)
      changed = true
    } else if (!f._attachments[att]) {
      need.push(v)
      changed = true
    }
  }

  if(changed){
    change.log('CHANGED TRUE')
  }

  change.log('need', need)

  // remove any versions that s removes, or which lack attachments
  for (var v in f.versions) {
    if (!s.versions[v]){
      change.log('deleting version', v)
      delete f.versions[v]
    }
  }

  for (var a in f._attachments) {
    var found = false
    for (var v in f.versions) {
      var tgz = f.versions[v].dist.tarball
      var b = path.basename(url.parse(tgz).pathname)
      if (b === a) {
        found = true
        change.log('found existing attachment', b)
        break
      }
    }
    if (!found) {
      change.log('deleting attachment', a)
      delete f._attachments[a]
      changed = true
    }
  }

  change.log('assigning to s._rev :', s._rev, "value of f._rev: ", f._rev)

  s._rev = fat_revision

  for (var k in s) {
    if (k !== '_attachments' && k !== 'versions') {
      if (changed)
        f[k] = s[k]
      else if (JSON.stringify(f[k]) !== JSON.stringify(s[k])) {
        f[k] = s[k]
        changed = true
      }
    }
  }

  changed = readmeTrim(f) || changed

  change.log('changes detected:', changed)

  if (!changed)
    this.resume()
  else
    this.fetchAll(change, need, [])
}

FullFat.prototype.put = function put(change, did) {
  change.log(arguments.callee.name)
  var f = change.fat
  change.did = did
  // at this point, all the attachments have been fetched into
  // {this.tmp}/{change.id}-{change.seq}/{attachment basename}
  // make a multipart PUT with all of the missing ones set to
  // follows:true
  var boundaries = []
  var boundary = this.boundary
  var bSize = 0

  var attSize = 0
  var atts = f._attachments = f._attachments || {}

  var new_atts = {}

  did.forEach(function(att) {

    new_atts[att.name] = {
      length: att.length,
      follows: true
    }

    if (att.type)
      new_atts[att.name].type = att.type
  })

  var send = []
  
  Object.keys(new_atts).forEach(function (name) {
    var att = new_atts[name]

    if (att.follows !== true)
      return

    send.push([name, att])
  })

  change.log('writing doc with rev: ', f._rev)

  // put with new_edits=false to retain the same rev
  // this assumes that NOTHING else is writing to this database!
  //var p = url.parse(this.fat + '/' + f.name + '?new_edits=false')
  var p = url.parse(this.fat + '/' + f.name + "?new_edits=false") //+ '?rev='+f._rev)
  delete f._revisions
  p.method = 'PUT'
  p.headers = {
    'user-agent': this.ua,
    'content-type': 'application/json',
    agent: false
  }

  var doc = new Buffer(JSON.stringify(f), 'utf8')
  var len = doc.length

  p.headers['content-length'] = len

  var req = hh.request(p)
  req.on('error', this.emit.bind(this, 'error'))

  dbg && change.log('writing doc:', JSON.stringify(JSON.parse(doc), null, 2))

  req.on('response', parse(_onPutDoc.bind(this, change)))
  req.end(doc)

  function _onPutDoc(change, er, data, res){
    change.log(arguments.callee.name)

    dbg && change.log(arguments.callee.name, data)

    if (!change.id)
      throw new Error('wtf?')

    // In some oddball cases, it looks like CouchDB will report stubs that
    // it doesn't in fact have.  It's possible that this is due to old bad
    // data in a past FullfatDB implementation, but whatever the case, we
    // ought to catch such errors and DTRT.  In this case, the "right thing"
    // is to re-try the PUT as if it had NO attachments, so that it no-ops
    // the attachments that ARE there, and fills in the blanks.
    // We do that by faking the onfatget callback with a 404 error.
    if (er && er.statusCode === 412 &&
        0 === er.message.indexOf('{"error":"missing_stub"')){

      change.log('412 error missing stub')
      this.emit('error', er)
      
    } else if (er){
      this.emit('error', er)
    } else {

      _putAttachments.call(this, change, send, data.rev)

    }
  }

  function _putAttachments(change, send, rev){

    var done = []
    var idx = 0

    if(change.did.length !== 0){
      _putOneAttachment.call(this, 0, rev, _attachmentDone)
    } else {
      this.resume()
    }

    function _putOneAttachment(idx, rev, doneCb){
      change.log(arguments.callee.name)
      
      change.log(arguments.callee.name, 'idx, send', idx, send[idx])
      change.log(arguments.callee.name, 'rev = ', rev)

      var f = change.fat

      var ns = send[idx]
      
      var name = ns[0]
      var att = ns[1]

      var p = url.parse(this.fat + '/' + f.name + '/' + name + '?&rev='+rev)
      
      p.method = 'PUT'
      p.headers = {
        'user-agent': this.ua,
        'content-type': att.type|| 'application/octet-steam',
        agent: false
      }

      var file = path.join(this.tmp, change.id + '-' + change.seq, name)
      //var data = fs.readFileSync(file)
      var rs = fs.createReadStream(file)

      var req = hh.request(p)
      req.on('error', this.emit.bind(this, 'error'))

      change.log('writing attachment', p.path)
      req.on('response', parse(doneCb.bind(this, name, att, idx)))

      rs.pipe(req)
      //req.end(data)

      // XXX req.setTimeout()
    }


    function _attachmentDone(name, att, idx, er, data, res){

      dbg && change.log('_attachmentDone(name, att, idx, er, data, res)', name, att, idx, er, data)
      
      if (er){
        this.emit('error', er)
      } else {
        this.emit('putAttachment', change, data, name, att)

        done.push(name)
        
        if(done.length === send.length){

          change.log('attachments done', done)

          rimraf(this.tmp + '/' + change.id + '-' + change.seq, function(err){
            change.log('rmrf done', err)
          })
          
          this.emit('put', change, done)
          
          this.resume()
        } else {
          _putOneAttachment.call(this, idx+1, data.rev, _attachmentDone)
        }
      }
    }

  }


}

FullFat.prototype.putAttachments0 = function putAttachments0(req, change, boundaries, send) {
 // send is the ordered list of [[name, attachment object],...]
  var b = boundaries.shift()
  var ns = send.shift()

  // last one!
  if (!ns) {
    change.log(arguments.callee.name, '---last')
    req.write(b, 'ascii')
    return req.end()
  }

  var name = ns[0]

  change.log(arguments.callee.name, name, 'start')

  req.write(b, 'ascii')
  var file = path.join(this.tmp, change.id + '-' + change.seq, name)
  var data = fs.readFileSync(file)
  //var fstr = fs.createReadStream(file)

  req.write(data, function(){
    change.log(arguments.callee.name, name, 'done')
    
    this.emit('upload', {
      change: change,
      name: name
    })
    
    this.putAttachments(req, change, boundaries, send)
    
  }.bind(this))

}


FullFat.prototype.onputdoc = function onputdoc(change, er, data, res) {
}


FullFat.prototype.onputres = function onputres(change, er, data, res) {
  change.log(arguments.callee.name)

  if (!change.id)
    throw new Error('wtf?')

  // In some oddball cases, it looks like CouchDB will report stubs that
  // it doesn't in fact have.  It's possible that this is due to old bad
  // data in a past FullfatDB implementation, but whatever the case, we
  // ought to catch such errors and DTRT.  In this case, the "right thing"
  // is to re-try the PUT as if it had NO attachments, so that it no-ops
  // the attachments that ARE there, and fills in the blanks.
  // We do that by faking the onfatget callback with a 404 error.
  if (er && er.statusCode === 412 &&
      0 === er.message.indexOf('{"error":"missing_stub"') &&
      !change.didFake404){
    change.didFake404 = true
    this.onfatget(change, { statusCode: 404 }, {}, {})
  } else if (er)
    this.emit('error', er)
  else {
    this.emit('put', change, data)
    // Just a best-effort cleanup.  No big deal, really.
    rimraf(this.tmp + '/' + change.id + '-' + change.seq, function() {})
    this.resume()
  }
}

FullFat.prototype.fetchAll = function fetchAll(change, need, did) {
  change.log(arguments.callee.name)
  var f = change.fat
  var tmp = path.resolve(this.tmp, change.id + '-' + change.seq)
  var len = need.length
  if (!len)
    return this.put(change, did)

  var errState = null

  mkdirp(tmp, function(er) {
    if (er)
      return this.emit('error', er)
    need.forEach(this.fetchOne.bind(this, change, need, did))
  }.bind(this))
}

FullFat.prototype.fetchOne = function fetchOne(change, need, did, v) {
   change.log(arguments.callee.name)
  var f = change.fat
  var r = url.parse(change.doc.versions[v].dist.tarball)
  if (this.registry) {
    var p = '/' + change.id + '/-/' + path.basename(r.pathname)
    r = url.parse(this.registry + p)
  }

  r.method = 'GET'
  r.headers = {
    'user-agent': this.ua,
    agent: false
  }

  var req = hh.request(r)
  req.on('error', this.emit.bind(this, 'error'))
  req.on('response', this.onattres.bind(this, change, need, did, v, r))
  req.end()

  this.retryReq(req, this.fetchOne.bind(this, change, need, did, v))
}

FullFat.prototype.onattres = function onattres(change, need, did, v, r, res) {
  change.log(arguments.callee.name)
  var f = change.fat
  var att = r.href
  var sum = f.versions[v].dist.shasum
  var filename = f.name + '-' + v + '.tgz'
  var file = path.join(this.tmp, change.id + '-' + change.seq, filename)

  // TODO: If the file already exists, get its size.
  // If the size matches content-length, get the md5
  // If the md5 matches content-md5, then don't bother downloading!

  function skip() {
    rimraf(file, function() {})
    delete f.versions[v]
    if (f._attachments)
      delete f._attachments[file]
    need.splice(need.indexOf(v), 1)
    maybeDone(null)
  }

  var maybeDone = function maybeDone(a) {
    if (a)
      this.emit('download', a)
    if (need.length === did.length)
      this.put(change, did)
  }.bind(this)

  // if the attachment can't be found, then skip that version
  // it's uninstallable as of right now, and may or may not get
  // fixed in a future update
  if (res.statusCode !== 200) {
    var er = new Error('Error fetching attachment: ' + att)
    er.statusCode = res.statusCode
    er.code = 'attachment-fetch-fail'
    if (this.missingLog)
      return fs.appendFile(this.missingLog, att + '\n', skip)
    else
      return this.emit('error', er)
  }

  var fstr = fs.createWriteStream(file)

  // check the shasum while we're at it
  var sha = crypto.createHash('sha1')
  var shaOk = false
  var errState = null

  sha.on('data', function(c) {
    c = c.toString('hex')
    if (c === sum)
      shaOk = true
  }.bind(this))

  if (!res.headers['content-length']) {
    var counter = new Counter()
    res.pipe(counter)
  }

  res.pipe(sha)
  res.pipe(fstr)

  fstr.on('error', function(er) {
    er.change = change
    er.version = v
    er.path = file
    er.url = att
    this.emit('error', errState = errState || er)
  }.bind(this))

  fstr.on('close', function() {
    if (errState || !shaOk) {
      // something didn't work, but the error was squashed
      // take that as a signal to just delete this version
      return skip()
    }
    // it worked!  change the dist.tarball url to point to the
    // registry where this is being stored.  It'll be rewritten by
    // the _show/pkg function when going through the rewrites, anyway,
    // but this url will work if the couch itself is accessible.
    var newatt = this.publicFat + '/' + change.id +
                 '/' + change.id + '-' + v + '.tgz'
    f.versions[v].dist.tarball = newatt

    if (res.headers['content-length'])
      var cl = +res.headers['content-length']
    else
      var cl = counter.count

    var a = {
      change: change,
      version: v,
      name: path.basename(file),
      length: cl,
      type: res.headers['content-type']
    }
    did.push(a)
    maybeDone(a)

  }.bind(this))
}

FullFat.prototype.destroy = function() {
  if (this.follow)
    this.follow.die()
}

FullFat.prototype.pause = function() {
  if (this.follow)
    this.follow.pause()
}

FullFat.prototype.resume = function() {
  this.writeSeq()
  if (this.follow)
    this.follow.resume()
}

util.inherits(Counter, stream.Writable)
function Counter(options) {
  stream.Writable.call(this, options)
  this.count = 0
}
Counter.prototype._write = function(chunk, encoding, cb) {
  this.count += chunk.length
  cb()
}
