import subprocess, sys, getopt, time,shlex,shutil
from subprocess import Popen
import os
import random
import cPickle, zlib
import filecmp
import sha

class HighLevelStorage(object):
    def __init__(self, path, engines=[]):
        self.path = path
        self.engines = engines

    def filename(self, id):
        r = str(id).split("_HASH_")[0]
        return r + ".dat"
    
    def attach_hash(self, id, hash):
        return str(id) + "_HASH_" + str(hash)

    def hash(self, id):
        r = str(id).split("_HASH_")
        if len(r) > 1:
            return r[-1]
        else:
            return None

    def tmp_filename(self):
        x = random.randint(0,1000000000000)
        id = "tmp" + str(x) 
        while(os.path.isfile(self.filename(id))):
            x = random.randint(0,1000000000000)
            id = "tmp" + str(x) 
        return self.filename(id)
       
    def create_unique_id(self):
        """Create unique file id"""
        id = None
        while(self.exists(id)):
            x = random.randint(0,1000000000000)
            id = "object" + str(x) 
        return id

    def exists(self, id):
        """Check if id is in use"""
        if id is None:
            return True
        filename = self.filename(id)
        return any([engine.is_file(filename) for engine in self.engines])
    
    def submit(self, object,id=None,noreplace=False):
        """Submit object to grid. Returns id.
        
        @param id: Give a fixed id. 
        @param noreplace: if fixed id given, an file exists, return just id [default=False]
        """
        data = zlib.compress(cPickle.dumps(object,protocol=2))

        local_filename = self.tmp_filename()
        f = open(local_filename,'w')
        f.write(data)
        f.close()
        
        try:
            id = self.submit_file(local_filename, id, noreplace)
        finally: 
            os.remove(local_filename)
        return id

    def hash_file(self, filename):
        hash = sha.new()
        f = open(filename, 'r+b')
        x = f.read(4096)
        while x:
            hash.update(x)
            x = f.read(4096)
        return hash.hexdigest()

    def submit_file(self, source_filename,id=None,noreplace=False):
        """As `submit`, but for files"""

        hash = self.hash_file(source_filename)

        if(not id):
            id = create_unique_id()

        filename = self.filename(id)
        
        
        for engine in self.engines:
            if noreplace and engine.is_file(filename):
                continue
            engine.store_file(source_filename, filename)
            try:
                engine.replicate_all(filename)
            except Exception, e:
                pass
       
        id = self.attach_hash(id,hash)
        return id

    def receive(self, id):
        """Receive object stored as `id`."""
        retry = 3
        filename = None
        while(retry):
            try:
                filename = self.receive_file(id)
                f = open(filename,'r')
                data = f.read()
                f.close()
                object = cPickle.loads(zlib.decompress(data))
                retry = 0
            except Exception,e:
                if filename is None or self.hash(id): #receive_file failed, or problem with unpickling. no point in retrying
                    raise e
                #receive file did get data, but it might be corrupted. Lets retry
                if os.path.isfile(filename):
                    os.remove(filename)
                retry -= 1
                if(retry == 0):
                    raise RuntimeError, str(e) + " for file " + self.filename(id)
                else:
                    print "Retry receive for file " + str(id)

        return object

    def receive_file(self, id):
        """Retrieves file. Returns filename"""
        filename = self.filename(id)
        local_filename = os.path.join(self.path, filename)

        if os.path.isfile(local_filename):
            if self.hash(id) and not self.hash(id) == self.hash_file(local_filename):
                os.remove(local_filename) #maybe an old version? Lets try to reload before complaining.
            else:                
                return local_filename
       
        last_error = None
        tmpfile = self.tmp_filename()
        retry = 3
        while retry:
            for engine in self.engines:
                if not engine.is_file(filename):
                    continue
                try:                
                    engine.retrieve_file(filename, tmpfile)
                    break
                except Exception, e:
                    print "File " + self.filename(id) + " could not be retrieved by " + str(engine) + ". Falling back to next engine."
                    last_error = e
            else:
                if last_error is None:
                    raise RuntimeError, "File " + self.filename(id) + " not found by any storage engines"
                else:
                    raise e

            if self.hash(id):
                if self.hash(id) == self.hash_file(tmpfile):
                    retry = 0
                else:
                    retry -= 1
                    os.remove(tmpfile)
                    if retry == 0:
                        raise RuntimeError, "File " + self.filename(id) + " retrieved with wrong hash"
                    else:                        
                        print "Retry receive for file " + str(id)
            else:
                retry = 0
        try:
            os.rename(tmpfile, local_filename)
        except OSError:
            pass
        return local_filename

    def destroy(self, id):
        """Destroys file with `id` on grid"""
        filename = self.filename(id)
        for engine in self.engines:
            try:
                engine.delete_file(filename)
            except Exception, e:
                print "Failed removal of " + self.filename(id) + " by " + str(engine)


    def destroy_all(self, only_unknown=True):
        """Destroy all files ending with .dat that are in path.
        @param only_unknown: destroy only those with auto-generated id
        (i.e. starting name wih object) [default: False]"""
        
        files = set()
        for engine in self.engines:
            files.update(engine.list_dir())

        files =  [file[:-4] for file in files if file.endswith('.dat')]

        if(only_unknown):
            files =  [file for file in files if file.startswith('object')]
        
        for pos,file in enumerate(files):
            print str(pos) + "/" + str(len(files)) + ": " + file
            self.destroy(file)


def _robust_process(command,times=3,noerror=None, **kwargs):
    retry=times
    args = shlex.split(command)
    while(retry):
        try:
            out,err = Popen(args,stdout=subprocess.PIPE,stderr=subprocess.PIPE,**kwargs).communicate()
            if(err):
                if(noerror and noerror in err):
                    return False
                else:
                    raise RuntimeError, err
            retry = 0
        except Exception,e:
            retry-=1
            if(retry == 0):
                raise e
            else:
                print "RETRY: " + str(e)
                time.sleep((times - retry) * 30)
    return out
    
def _robust_func(func,times=3, *args, **kwargs):
    retry=times
    while(retry):
        try:
            res = func(*args,**kwargs)
            retry = 0
        except Exception,e:
            retry-=1
            if(retry == 0):
                raise e
            else:
                time.sleep((times - retry) * 30)
    return res

class StoragePath(object):
    def __init__(self, engine, path=None):
        self.engine = engine
        self.path = os.path.expanduser(path)

    def newpath(self, filename):
        return os.path.abspath(os.path.join(self.path, filename))

    def is_file(self, filename):
        return self.engine.is_file(self.newpath(filename))

    def list_dir(self, cpath=""):
        return self.engine.list_dir(self.newpath(cpath))

    def mkdir(self, cpath):
        return self.engine.mkdir(self.newpath(filename))
    
    def rmdir(self, cpath):
        return self.engine.rmdir(self.newpath(filename))

    def store_file(self,filepath,cpath,check_equal=True):
        return self.engine.store_file(filepath, self.newpath(cpath))

    def retrieve_file(self,cpath,filepath):
        return self.engine.retrieve_file(self.newpath(cpath), filepath)
     
    def replicate_all(self,cpath):
        return self.engine.replicate_all(self.newpath(cpath))

    def delete_file(self, filename):
        return self.engine.delete_file(self.newpath(filename))

class LocalStorageEngine(object):
    def list_dir(self, cpath):
        return os.listdir(cpath)

    def mkdir(self, cpath):
        os.mkdir(cpath)
    
    def rmdir(self, cpath):
        os.rmdir(cpath)

    def store_file(self,filepath,cpath,check_equal=True):
        if filepath != cpath:
            shutil.copyfile(filepath, cpath)

    def retrieve_file(self,cpath,filepath):
        if filepath != cpath:
            shutil.copyfile(cpath, filepath)
     
    def replicate_all(self,cpath):
        pass

    def is_file(self,cpath):
        return os.path.isfile(cpath)

    def delete_file(self, filename):
        os.remove(filename)


class ClusterStorageEngine(object):
    def __init__(self):
        self.storage_engine = os.environ['VO_LSGRID_DEFAULT_SE']

    def get_other_storage_engines(self):
        if(not 'storage_engines' in self.__dict__):
            command1 = "lcg-infosites --vo lsgrid se"
            command2 = "grep -Po '\\b\\S+$'"
            command3 = "grep '\\.'"
            p1 = Popen(shlex.split(command1),stdout=subprocess.PIPE)
            p2 = Popen(shlex.split(command2),stdout=subprocess.PIPE,stdin=p1.stdout)
            storage_engines = _robust_process(command3,stdin=p2.stdout,times=1)
            storage_engines = set(storage_engines.split('\n'))
            storage_engines.discard('n.a')
            storage_engines.discard('SEs')
            storage_engines.discard('')
            storage_engines.discard(self.storage_engine)
            self.storage_engines = storage_engines
        return self.storage_engines

    def list_dir(self,cpath):
        cmd = "lcg-ls lfn:" + cpath
        files = _robust_process(cmd)
        files = files.split('\n')
        return files[:-1]

    def mkdir(self,cpath):
        cmd = "lfc-mkdir " + cpath
        _robust_process(cmd)
    
    def rmdir(self, cpath):
        cmd = "lfc-rm -r " + cpath
        _robust_process(cmd)

    def store_file(self,filepath,cpath,check_equal=True):
        filepath = os.path.abspath(filepath)
        if(not os.path.isfile(filepath)):
            raise AttributeError, "Filepath should be existing file"

        fpath,fname = os.path.split(filepath)

        if(cpath[-1] == '/'):
            gridname = os.path.join(cpath,fname)
        else:
            gridname = cpath
        
        #determine if a file with same name exists on storage
        if(self.is_file(gridname)):
            self.delete_file(gridname)

        command = 'lcg-cr --vo lsgrid -l "lfn:' + gridname + '" -d ' + self.storage_engine + ' "file:/' + filepath + '"'
        
        retry = 3
        while(retry):
            try:
                _robust_process(command)
                if(check_equal):
                    test_filepath = filepath + "._test_"
                    self.retrieve_file(gridname,test_filepath)
                    if not filecmp.cmp(filepath,test_filepath,shallow=False):
                        try:
                            self.delete_file(gridname)
                        except Exception, e:
                            pass
                        raise RuntimeError, "Files unequal"
                    os.remove(test_filepath)
                retry = 0
            except Exception, e:
                retry -= 1
                if(retry == 0):
                    raise e
                else:
                    time.sleep((3-retry) * 30)

    def retrieve_file(self,cpath,filepath):
        gpath,gname = os.path.split(cpath)
        
        if(os.path.isdir(filepath)):
            filepath = os.path.join(filepath,gname)
        filepath = os.path.abspath(filepath)
        gridname = cpath

        #determine if file exists on local storage engine
        command = 'lcg-lr "lfn:' + gridname  + '"'
        try:
            storage_locs = _robust_process(command)
        except RuntimeError:
            storage_locs = ""
       
        storage_locs = storage_locs.split('\n')
        sel_loc = [sloc for sloc in storage_locs if self.storage_engine in sloc]
        retry = 3
        while(retry):
            try:
                if(retry < 3 and storage_locs):
                    command = 'lcg-cp --vo lsgrid ' + storage_locs[random.randint(0,len(storage_locs))] + ' "file:/' + filepath + '"'
                elif(not sel_loc):
                    command = 'lcg-cp --vo lsgrid "lfn:' + gridname + '" "file:/' + filepath + '"'
                else:
                    command = 'lcg-cp --vo lsgrid ' + sel_loc[0] + ' "file:/' + filepath + '"'
               
                _robust_process(command)
                retry = 0
            except Exception, e:
                retry -= 1
                if(retry == 0):
                    raise RuntimeError, str(e) + " on cpath " + cpath
      
    def replicate_all(self,cpath):
        self.other_engines = _robust_func(self.get_other_storage_engines)

        commands = []
        for engine in self.other_engines:
            command = 'lcg-rep --vo lsgrid -d ' + engine + ' "lfn:' + cpath + '"'
            commands.append(command)

        retry = 3
        while(retry):
            ncommands = []
            processes = []
            for command in commands:
                args = shlex.split(command)
                processes.append((Popen(args,stdout=subprocess.PIPE,stderr=subprocess.PIPE),command))
        
            for process,command in processes:
                r = process.wait()
                if(r):
                    ncommands.append(command)
            
            if(ncommands):
                retry -= 1
                commands = ncommands
                if retry > 0:
                    time.sleep((3 - retry) * 30)
            else:
                retry = 0
        
        if(ncommands):
            raise RuntimeError, "Failed replication with " + str(ncommands)


    def is_file(self,cpath):
        #determine if file exists on local storage engine
        command = 'lcg-ls "lfn:' + cpath + '"' 

        r = _robust_process(command,noerror="No such file or directory")
        return not r is False

    def delete_file(self,gridname):
        command = 'lcg-del -a "lfn:' + gridname + '"'
        _robust_process(command)

if 'VO_LSGRID_DEFAULT_SE' in os.environ:
    cs = ClusterStorageEngine()
    
ls = LocalStorageEngine()
lsp = StoragePath(ls, os.getcwd())

if 'LFC_HOME' in os.environ and 'VO_LSGRID_DEFAULT_SE' in os.environ:
    csp = StoragePath(cs, os.environ['LFC_HOME'])
    hs = HighLevelStorage(os.getcwd(), [csp, lsp])
else:
    hs = HighLevelStorage(os.getcwd(), [lsp])
   
create_unique_id = hs.create_unique_id
exists = hs.exists
submit = hs.submit
submit_file = hs.submit_file
receive = hs.receive
receive_file = hs.receive_file
destroy = hs.destroy
destroy_all = hs.destroy_all

