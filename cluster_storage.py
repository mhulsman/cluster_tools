import subprocess, sys, getopt, time,shlex
from subprocess import Popen
import os
import random
import cPickle, zlib
import filecmp



def check_id(id):
    if(not id):
        return False
    filename = id + ".dat"
    if(os.path.isfile(filename)):
        return False
    return not cs.is_file(os.environ['LFC_HOME'] + "/" + filename)

def create_unique_id():
    id = None
    while(not check_id(id)):
        x = random.randint(0,100000000)
        id = "object" + str(x) 
    return id

def submit(object,id=None,noreplace=False):
    if(not id):
        id = create_unique_id()
    filename = id + ".dat"
    network_path = os.environ['LFC_HOME'] + '/' + filename

    if(noreplace):
        if(os.path.isfile(filename) and cs.is_file(network_path)):
            return id

    data = zlib.compress(cPickle.dumps(object,protocol=2))
    if(os.path.isfile(filename)):
        os.remove(filename)

    f = open(filename,'w')
    f.write(data)
    f.close()
    
    cs.store_file(filename,network_path)
    try:
        cs.replicate_all(network_path)
    except Exception, e:
        pass
    return id

def submit_file(source_filename,id=None,noreplace=False):
    if(not id):
        id = create_unique_id()
    
    filename = id + ".dat"
    network_path = os.environ['LFC_HOME'] + '/' + filename

    if(noreplace):
        if(cs.is_file(network_path)):
            return id

    cs.store_file(source_filename,network_path)
    try:
        cs.replicate_all(network_path)
    except Exception, e:
        pass
    return id

def receive(id):
    filename = id + ".dat"
    retry = 3
    while(retry):
        try:
            if(not os.path.isfile(filename)):
                network_path = os.environ['LFC_HOME'] + '/' + filename
                cs.retrieve_file(network_path,filename)
            
            f = open(filename,'r')
            data = f.read()
            f.close()
            object = cPickle.loads(zlib.decompress(data))
            retry = 0
        except Exception,e:
            retry -= 1
            if(os.path.isfile(filename)):
                os.remove(filename)
            if(retry == 0):
                print "FAIL RECEIVE"
                raise RuntimeError, str(e) + " on id " + id
            else:
                print "RETRY RECEIVE"
            time.sleep(30)

    return object

def receive_file(id):
    filename = id + ".dat"
    if(not os.path.isfile(filename)):
        network_path = os.environ['LFC_HOME'] + '/' + filename
        cs.retrieve_file(network_path,filename)
    return filename

def destroy(id, check_grid_exist=True):
    try:
        filename = id + ".dat"
        if(os.path.isfile(filename)):
            os.remove(filename)
        network_path = os.environ['LFC_HOME'] + '/' + filename
        if(not check_grid_exist or cs.is_file(network_path)):
            cs.delete_file(filename)
    except Exception, e:
        print "FAIL DESTROY of" + id

def destroy_all(only_unknown=False):
    files = cs.list_dir(os.environ['LFC_HOME'])

    files =  [file[:-4] for file in files if file.endswith('.dat')]
    if(only_unknown):
        files =  [file for file in files if file.startswith('object')]
        
    
    for pos,file in enumerate(files):
        print str(pos) + "/" + str(len(files)) + ": " + file
        destroy(file)


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
                time.sleep(30)
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
                time.sleep(30)
    return res

class ClusterStorage(object):
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
        pass

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
                    time.sleep(30)

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
                time.sleep(30)
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

cs = ClusterStorage()
