#! /usr/bin/env python
"""
Script for automated imaging / ripping of optical media using a Nimbie disc robot.

Features:

* Automated load / unload  / reject using dBpoweramp driver binaries
* Disc type detection using libcdio's cd-info tool
* Data CDs and DVDs are imaged to ISO file using IsoBuster
* Audio CDs are ripped to WAV or FLAC using dBpoweramp

Author: Johan van der Knijff
Research department,  KB / National Library of the Netherlands

"""

import sys
import os
import csv
import importlib
import time
import glob
import xml.etree.ElementTree as ETree
import threading
import uuid
import logging
import json
import queue
import tkinter as tk
from tkinter import filedialog as tkFileDialog
from tkinter import scrolledtext as ScrolledText
from tkinter import messagebox as tkMessageBox
from tkinter import ttk
from . import config
from . import cdworker
from . import cdinfo


__version__ = '1.0.1'
config.version = __version__

class carrierEntry(tk.Frame):

    """This class defines the graphical user interface + associated functions
    for associated actions
    """

    def __init__(self, parent, *args, **kwargs):
        """Initiate class"""
        tk.Frame.__init__(self, parent, *args, **kwargs)
        self.root = parent
        # Logging stuff
        self.logger = logging.getLogger()
        # Create a logging handler using a queue
        self.log_queue = queue.Queue(-1)
        self.queue_handler = QueueHandler(self.log_queue)
        config.readyToStart = False
        config.finishedBatch = False
        self.carrierNumber = 0
        self.build_gui()

    def on_quit(self, event=None):
        """Wait until the disc that is currently being pocessed has
        finished, and quit (batch can be resumed by opening it in the File dialog)
        """
        config.quitFlag = True
        self.bExit.config(state='disabled')
        self.bFinalise.config(state='disabled')
        if config.batchIsOpen:
            msg = 'User pressed Exit, quitting after current disc has been processed'
            tkMessageBox.showinfo("Info", msg)
        if not config.readyToStart:
            # Wait 2 seconds to avoid race condition
            time.sleep(1)
            msg = 'Quitting because user pressed Exit, click OK to exit'
            tkMessageBox.showinfo("Exit", msg)
            os._exit(0)

    def on_create(self, event=None):
        """Create new batch in rootDir"""

        # Create unique batch identifier (UUID, based on host ID and current time)
        # this ensures that if the software is run in parallel on different machines
        # the batch identifiers will always be unique

        self.bNew.config(state='disabled')
        self.bOpen.config(state='disabled')
        self.batch_button.config(state='normal')
        self.batchTypeMenu.config(state='normal')
        self.coll_entry.config(state='normal')
        self.coll_entry.focus_set()
        self.staff_name.config(state='normal')
        self.batchID = str(uuid.uuid1())

    def on_batchinfo(self, event=None):

        if not (self.coll_entry.get()[0] == 'M') and not representsInt(self.coll_entry.get()[1:]):
            msg = "Collection ID should be M#####"
            tkMessageBox.showerror("Error", msg)
            self.on_create()
        elif not self.staff_name.get():
            msg = "Please fill in the Staff Name field"
            tkMessageBox.showerror("Error mismatch", msg)
            self.on_create()
        else:
            # Construct batch name
            self.batchName = self.coll_entry.get().strip() + '-' + self.batchID
            config.batchName = self.batchName
            config.batchFolder = os.path.join(config.rootDir, self.batchName)
            try:
                os.makedirs(config.batchFolder)
            except IOError:
                msg = 'Cannot create batch folder ' + config.batchFolder
                tkMessageBox.showerror("Error", msg)

            # Create jobs folder
            config.jobsFolder = os.path.join(config.batchFolder, 'jobs')
            try:
                os.makedirs(config.jobsFolder)
            except IOError:
                msg = 'Cannot create jobs folder ' + config.jobsFolder
                tkMessageBox.showerror("Error", msg)

            # Create failed jobs folder (if a job fails it will be moved here)
            config.jobsFailedFolder = os.path.join(config.batchFolder, 'jobsFailed')
            try:
                os.makedirs(config.jobsFailedFolder)
            except IOError:
                msg = 'Cannot create failed jobs folder ' + config.jobsFailedFolder
                tkMessageBox.showerror("Error", msg)

            config.batchType = self.batchType.get()
            config.staffName = self.staff_name.get()

            # Set up logging
            successLogger = True

            try:
                self.setupLogger()
                # Start polling log messages from the queue
                self.after(100, self.poll_log_queue)
            except OSError:
                # Something went wrong while trying to write to lof file
                msg = ('error trying to write log file')
                tkMessageBox.showerror("ERROR", msg)
                successLogger = False

            if successLogger:
                # Notify user
                msg = 'Created batch ' + self.batchName
                tkMessageBox.showinfo("Created batch", msg)
                logging.info(''.join(['batchFolder set to ', config.batchFolder]))

                # Update state of buttons / widgets
                self.bNew.config(state='disabled')
                self.bOpen.config(state='disabled')
                self.batch_button.config(state='disabled')
                self.batchTypeMenu.config(state='disabled')
                self.coll_entry.config(state='disabled')
                self.staff_name.config(state='disabled')
                self.bFinalise.config(state='normal')
                self.media_entry.config(state='normal')
                self.media_entry.delete(0, tk.END)
                self.media_entry.insert(tk.END, "1")
                self.submit_button.config(state='normal')

                # Flag that is True if batch is open
                config.batchIsOpen = True
                # Set readyToStart flag to True, except if startOnFinalize flag is activated,
                # in which case readyToStart is set to True on finalisation
                if not config.startOnFinalize:
                    config.readyToStart = True


    def on_open(self, event=None):
        """Open existing batch"""

        # defining options for opening a directory
        self.dir_opt = options = {}
        options['initialdir'] = config.rootDir
        options['mustexist'] = True
        options['parent'] = self.root
        options['title'] = 'Select batch directory'
        config.batchFolder = tkFileDialog.askdirectory(**self.dir_opt)
        config.jobsFolder = os.path.join(config.batchFolder, 'jobs')
        config.jobsFailedFolder = os.path.join(config.batchFolder, 'jobsFailed')

        # Check if batch was already finalized, and exit if so 
        if not os.path.isdir(config.jobsFolder):
            msg = 'cannot open finalized batch'
            tkMessageBox.showerror("Error", msg)
        else:
            # Set up logging
            successLogger = True

            try:
                self.setupLogger()
                # Start polling log messages from the queue
                self.after(100, self.poll_log_queue)
            except OSError:
                # Something went wrong while trying to write to lof file
                msg = ('error trying to write log file')
                tkMessageBox.showerror("ERROR", msg)
                successLogger = False

            if successLogger:
                logging.info(''.join(['*** Opening existing batch ', config.batchFolder, ' ***']))

                if config.batchFolder != '':
                    # Import info on jobs in queue to the treeview widget

                    # Get directory listing of job files sorted by creation time
                    jobFiles = list(filter(os.path.isfile, glob.glob(config.jobsFolder + '/*')))
                    jobFiles.sort(key=lambda x: os.path.getctime(x))
                    jobCount = 1

                    for job in  jobFiles:
                         # Open job file, read contents to list
                        fj = open(job, "r", encoding="utf-8")
                        fjCSV = csv.reader(fj)
                        jobList = next(fjCSV)
                        fj.close()

                        if jobList[0] != 'EOB':
                            media_id = jobList[1]
                            status = jobList[2]

                            # Add Media ID number to treeview widget
                            self.tv.insert('', 0, text=str(jobCount), values=(media_id, status))
                            jobCount += 1

                    # Update state of buttons /widgets, taking into account whether batch was
                    # finalized by user
                    self.bNew.config(state='disabled')
                    self.bOpen.config(state='disabled')
                    self.batch_button.config(state='disabled')
                    self.batchTypeMenu.config(state='disabled')
                    self.coll_entry.config(state='disabled')
                    self.staff_name.config(state='disabled')
                    if os.path.isfile(os.path.join(config.jobsFolder, 'eob.txt')):
                        self.bFinalise.config(state='disabled')
                        self.submit_button.config(state='disabled')
                    else:
                        self.bFinalise.config(state='normal')
                        self.submit_button.config(state='normal')
                    self.media_entry.config(state='normal')
                    self.media_entry.delete(0, tk.END)
                    self.media_entry.insert(tk.END, "1")

                    # Flag that is True if batch is open
                    config.batchIsOpen = True
                    # Set readyToStart flag to True, except if startOnFinalize flag is activated,
                    # in which case readyToStart is set to True on finalisation
                    if not config.startOnFinalize:
                        config.readyToStart = True

    def on_finalise(self, event=None):
        """Finalise batch after user pressed finalise button"""
        msg = ("This will finalise the current batch.\n After finalising no further"
               "carriers can be \nadded. Are you really sure you want to do this?")
        if tkMessageBox.askyesno("Confirm", msg):
            # Create End Of Batch job file; this will tell the main worker processing
            # loop to stop

            jobFile = 'eob.txt'
            fJob = open(os.path.join(config.jobsFolder, jobFile), "w", encoding="utf-8")
            lineOut = 'EOB\n'
            fJob.write(lineOut)
            self.bFinalise.config(state='disabled')
            self.submit_button.config(state='disabled')
            self.media_entry.delete(0, tk.END)
            self.media_entry.config(state='disabled')
            
            # If the startOnFinalize option was activated, set readyToStart flag to True
            if config.startOnFinalize:
                config.readyToStart = True

    def on_submit(self, event=None):
        """Process one record and add it to the queue after user pressed submit button"""
        # Fetch entered values (strip any leading / tralue whitespace characters)
        self.current_coll = self.coll_entry.get().strip() 
        self.current_media_entry = self.media_entry.get().strip()

        self.current_media_id = self.coll_entry.get().strip() + '_' + self.current_media_entry.zfill(4)


        if not config.batchIsOpen:
            msg = "You must first create a batch or open an existing batch"
            tkMessageBox.showerror("Not ready", msg)
        elif not representsInt(self.current_media_entry):
            msg = "Volume number must be integer value"
            tkMessageBox.showerror("Type mismatch", msg)
        elif int(self.current_media_entry) < 1:
            msg = "Volume number must be greater than or equal to 1"
            tkMessageBox.showerror("Value error", msg)
        else:
            msg = ("Please load disc ('" + self.current_media_entry +
                   ") into the disc loader, then press 'OK'")
            tkMessageBox.showinfo("Load disc", msg)

            # Create unique identifier for this job (UUID, based on host ID and current time)
            jobID = str(uuid.uuid1())
            # Create and populate Job file
            jobFile = os.path.join(config.jobsFolder, self.current_media_id + '-' + jobID + ".txt")

            fJob = open(jobFile, "w", encoding="utf-8")

            # Create CSV writer object
            jobCSV = csv.writer(fJob, lineterminator='\n')

            # Row items to list
            rowItems = ([jobID, self.current_coll ,self.current_media_id, 'queued'])

            # Write row to job and close file
            jobCSV.writerow(rowItems)
            fJob.close()

            # Display Title + Volume number in treeview widget
            self.carrierNumber += 1
            self.tv.insert('', 0, text=str(self.current_media_id), values=(self.carrierNumber, 'queued'))

            # Reset entry fields and set focus on Title field
            self.media_entry.delete(0, tk.END)
            self.media_entry.insert(tk.END, int(self.current_media_entry) + 1)
            self.media_entry.focus_set()


    def update_treeview(self):
        """Update interface with completion/fail"""
        tv_items = self.tv.get_children()
        if tv_items:
            failed = [x.split('.')[0] for x in os.listdir(config.jobsFailedFolder)]
            queued = [x.split('-')[0] for x in os.listdir(config.jobsFolder)]
            for item in tv_items:
                media_id = self.tv.item(item)["text"]
                values = self.tv.item(item)["values"]

                if media_id in queued:
                    status = 'queued'
                elif media_id in failed:
                    status = 'failed'
                else:
                    status = 'completed'

                values[1] = status
                self.tv.item(item, values = values)


    def setupLogger(self):
        """Set up logging-related settings"""
        logFile = os.path.join(config.batchFolder, self.batchName + '.log')

        logging.basicConfig(handlers=[logging.FileHandler(logFile, 'a', 'utf-8')],
                            level=logging.INFO,
                            format='%(asctime)s - %(levelname)s - %(message)s')

        # Add the handler to logger
        self.logger = logging.getLogger()
        # This sets the console output format (slightly different from basicConfig!)
        formatter = logging.Formatter('%(levelname)s: %(message)s')
        self.queue_handler.setFormatter(formatter)
        self.logger.addHandler(self.queue_handler)


    def display(self, record):
        """Display log record in scrolledText widget"""
        msg = self.queue_handler.format(record)
        self.st.configure(state='normal')
        self.st.insert(tk.END, msg + '\n', record.levelname)
        self.st.configure(state='disabled')
        # Autoscroll to the bottom
        self.st.yview(tk.END)


    def poll_log_queue(self):
        """Check every 100ms if there is a new message in the queue to display"""
        while True:
            try:
                record = self.log_queue.get(block=False)
            except queue.Empty:
                break
            else:
                self.display(record)
        self.after(100, self.poll_log_queue)


    def build_gui(self):
        """Build the GUI"""
        
        # Read configuration file
        getConfiguration()
        
        self.root.title('iromlab v.' + config.version)
        self.root.option_add('*tearOff', 'FALSE')
        self.grid(column=0, row=0, sticky='ew')
        self.grid_columnconfigure(0, weight=1, uniform='a')
        self.grid_columnconfigure(1, weight=1, uniform='a')
        self.grid_columnconfigure(2, weight=1, uniform='a')
        self.grid_columnconfigure(3, weight=1, uniform='a')

        # Batch toolbar
        self.bNew = tk.Button(
            self,
            text="New Batch",
            height=2,
            width=4,
            underline=0,
            command=self.on_create
        )
        self.bNew.grid(column=0, row=1, sticky='ew')
        self.bOpen = tk.Button(
            self,
            text="Open Batch",
            height=2,
            width=4,
            underline=0,
            command=self.on_open
        )
        self.bOpen.grid(column=1, row=1, sticky='ew')
        self.bFinalise = tk.Button(
            self,
            text="Finalize Batch",
            height=2,
            width=4,
            underline=0,
            command=self.on_finalise
        )
        self.bFinalise.grid(column=2, row=1, sticky='ew')
        self.bExit = tk.Button(
            self,
            text="Exit",
            height=2,
            width=4,
            underline=0,
            command=self.on_quit
        )
        self.bExit.grid(column=3, row=1, sticky='ew')

        # Disable finalise button on startup
        self.bFinalise.config(state='disabled')

        ttk.Separator(self, orient='horizontal').grid(column=0, row=2, columnspan=4, sticky='ew')

        # Batch info fields
        batchTypes = { 'Bags' , 'Disc Images' }
        self.batchType = tk.StringVar(self, 'Bags')
        tk.Label(self, text='Batch Type').grid(column = 0, row = 3, sticky='w')
        self.batchTypeMenu = tk.OptionMenu(self, self.batchType, *batchTypes)
        self.batchTypeMenu.grid(column =1, row = 3, sticky='ew', columnspan=2)

        tk.Label(self, text='Collection ID').grid(column=0, row=4, sticky='w')
        self.coll_entry = tk.Entry(self, width=45, state='disabled')
        self.coll_entry.grid(column=1, row=4, sticky='w', columnspan=3)
        
        tk.Label(self, text='Staff Name').grid(column=0, row=5, sticky='w')
        self.staff_name = tk.Entry(self, width=45, state='disabled')
        self.staff_name.grid(column=1, row=5, sticky='w', columnspan=3)

        self.batch_button = tk.Button(
            self,
            text='Save Batch Info',
            height=2,
            width=4,
            underline=0,
            state='disabled',
            command=self.on_batchinfo
        )
        self.batch_button.grid(column=0, row=6, sticky='ew', columnspan=4)

        ttk.Separator(self, orient='horizontal').grid(column=0, row=7, columnspan=4, sticky='ew')

        # Disc entry
        tk.Label(self, text='Media ID Number').grid(column=0, row=8, sticky='w')
        self.media_entry = tk.Entry(self, width=5, state='disabled')
        self.media_entry.grid(column=1, row=8, sticky='w', columnspan=2)

        self.submit_button = tk.Button(
            self,
            text='Add Disc',
            height=2,
            width=4,
            underline=0,
            state='disabled',
            command=self.on_submit
        )
        self.submit_button.grid(column=0, row=9, sticky='ew', columnspan=4)

        ttk.Separator(self, orient='horizontal').grid(column=0, row=10, columnspan=4, sticky='ew')

        # Treeview widget displays info on entered carriers
        self.tv = ttk.Treeview(
            self,
            height=10,
            columns=('Discs in queue'))
        self.tv.heading('#0', text='Media ID')
        self.tv.heading('#1', text='Number in Queue')
        self.tv.heading('#2', text='Status')
        self.tv.column('#0', stretch=tk.YES, width=50)
        self.tv.column('#1', stretch=tk.YES, width=25)
        self.tv.column('#2', stretch=tk.YES, width=50)
        self.tv.grid(column=0, row=11, sticky='ew', columnspan=4)

        # ScrolledText widget displays logging info
        self.st = ScrolledText.ScrolledText(self, state='disabled', height=15)
        self.st.configure(font='TkFixedFont')
        self.st.grid(column=0, row=12, sticky='ew', columnspan=4)

        # Define bindings for keyboard shortcuts: buttons
        self.root.bind_all('<Control-Key-n>', self.on_create)
        self.root.bind_all('<Control-Key-o>', self.on_open)
        self.root.bind_all('<Control-Key-f>', self.on_finalise)
        self.root.bind_all('<Control-Key-q>', self.on_quit)
        self.root.bind_all('<Control-Key-a>', self.on_submit)

        # TODO keyboard shortcuts for Radiobox selections: couldn't find ANY info on how to do this!

        for child in self.winfo_children():
            child.grid_configure(padx=5, pady=5)

    def reset_gui(self):
        """Reset the GUI"""
        # Reset carrierNumber
        self.carrierNumber = 0
        # Clear items in treeview widget
        tvItems = self.tv.get_children()
        for item in tvItems:
           self.tv.delete(item)
        # Clear contents of ScrolledText widget
        # Only works if state is 'normal'
        self.st.config(state='normal')
        self.st.delete(1.0, tk.END)
        self.st.config(state='disabled')
        # Logging stuff
        self.logger = logging.getLogger()
        # Create a logging handler using a queue
        self.log_queue = queue.Queue(-1)
        self.queue_handler = QueueHandler(self.log_queue)
        config.readyToStart = False
        config.finishedBatch = False

        # Update state of buttons / widgets
        self.bNew.config(state='normal')
        self.bOpen.config(state='normal')
        self.bFinalise.config(state='disabled')
        self.bExit.config(state='normal')
        self.batchTypeMenu.config(state='disabled')
        self.coll_entry.config(state='disabled')
        self.staff_name.config(state='disabled')
        self.submit_button.config(state='disabled')
        self.media_entry.config(state='disabled')

class QueueHandler(logging.Handler):
    """Class to send logging records to a queue
    It can be used from different threads
    The ConsoleUi class polls this queue to display records in a ScrolledText widget
    Taken from https://github.com/beenje/tkinter-logging-text-widget/blob/master/main.py
    """

    def __init__(self, log_queue):
        super().__init__()
        self.log_queue = log_queue

    def emit(self, record):
        self.log_queue.put(record)


def representsInt(s):
    """Return True if s is an integer, False otherwise"""
    # Source: http://stackoverflow.com/a/1267145
    try:
        int(s)
        return True
    except ValueError:
        return False


def checkFileExists(fileIn):
    """Check if file exists and exit if not"""
    if not os.path.isfile(fileIn):
        msg = "file " + fileIn + " does not exist!"
        tkMessageBox.showerror("Error", msg)
        sys.exit()


def checkDirExists(dirIn):
    """Check if directory exists and exit if not"""
    if not os.path.isdir(dirIn):
        msg = "directory " + dirIn + " does not exist!"
        tkMessageBox.showerror("Error", msg)
        sys.exit()


def errorExit(error):
    """Show error message in messagebox and then exit after userv presses OK"""
    tkMessageBox.showerror("Error", error)
    sys.exit()


def main_is_frozen():
    """Return True if application is frozen (Py2Exe), and False otherwise"""
    return (hasattr(sys, "frozen") or  # new py2exe
            hasattr(sys, "importers") or  # old py2exe
            imp.is_frozen("__main__"))  # tools/freeze


def get_main_dir():
    """Return application (installation) directory"""
    if main_is_frozen():
        return os.path.dirname(sys.executable)
    return os.path.dirname(sys.argv[0])


def findElementText(elt, elementPath):
    """Returns element text if it exists, errorExit if it doesn't exist"""
    elementText = elt.findtext(elementPath)
    if elementText is None:
        msg = 'no element found at ' + elementPath
        errorExit(msg)
    else:
        return elementText


def getConfiguration():
    """ Read configuration file, make all config variables available via
    config.py and check that all file paths / executables exist.
    This assumes an non-frozen script (no Py2Exe!)
    """

    # Locate Windows profile directory
    userDir = os.environ['USERPROFILE']
    
    # Locate package directory
    packageDir = os.path.dirname(os.path.abspath(__file__))
    # Config directory
    configDirUser = os.path.join(userDir, 'iromlab')
    configFileUser = os.path.join(configDirUser, 'config.xml')
    # Tools directory
    toolsDirUser = os.path.join(packageDir, 'tools')

    # Check if user config file exists and exit if not
    if not os.path.isfile(configFileUser):
        print(configFileUser)
        msg = 'configuration file not found'
        errorExit(msg)

    # Read contents to bytes object
    try:
        fConfig = open(configFileUser, "rb")
        configBytes = fConfig.read()
        fConfig.close()
    except IOError:
        msg = 'could not open configuration file'
        errorExit(msg)

    # Parse XML tree
    try:
        root = ETree.fromstring(configBytes)
    except Exception:
        msg = 'error parsing ' + configFileUser
        errorExit(msg)

    # Create empty element object & add config contents to it
    # A bit silly but allows use of findElementText in etpatch

    configElt = ETree.Element("bogus")
    configElt.append(root)

    config.cdDriveLetter = findElementText(configElt, './config/cdDriveLetter')
    config.rootDir = findElementText(configElt, './config/rootDir')
    config.tempDir = findElementText(configElt, './config/tempDir')
    config.secondsToTimeout = findElementText(configElt, './config/secondsToTimeout')
    config.prefixBatch = findElementText(configElt, './config/prefixBatch')
    config.audioFormat = findElementText(configElt, './config/audioFormat')
    config.reportFormatString = findElementText(configElt, './config/reportFormatString')
    config.prebatchExe = findElementText(configElt, './config/prebatchExe')
    config.loadExe = findElementText(configElt, './config/loadExe')
    config.unloadExe = findElementText(configElt, './config/unloadExe')
    config.rejectExe = findElementText(configElt, './config/rejectExe')
    config.isoBusterExe = findElementText(configElt, './config/isoBusterExe')
    config.dBpowerampConsoleRipExe = findElementText(configElt, './config/dBpowerampConsoleRipExe')
 
    # For below configuration variables, use default value if value cannot be
    # read from config file (this ensures v1 will work with old config files)
    try:
        if findElementText(configElt, './config/startOnFinalize') == "True":
            config.startOnFinalize = True
        else:
            config.startOnFinalize = False
    except:
        pass

    # Normalise all file paths
    config.rootDir = os.path.normpath(config.rootDir)
    config.tempDir = os.path.normpath(config.tempDir)
    config.prebatchExe = os.path.normpath(config.prebatchExe)
    config.loadExe = os.path.normpath(config.loadExe)
    config.unloadExe = os.path.normpath(config.unloadExe)
    config.rejectExe = os.path.normpath(config.rejectExe)
    config.isoBusterExe = os.path.normpath(config.isoBusterExe)
    config.dBpowerampConsoleRipExe = os.path.normpath(config.dBpowerampConsoleRipExe)

    # Paths to pre-packaged tools
    config.shntoolExe = os.path.join(toolsDirUser, 'shntool', 'shntool.exe')
    config.flacExe = os.path.join(toolsDirUser, 'flac', 'win64', 'flac.exe')
    config.cdInfoExe = os.path.join(toolsDirUser, 'libcdio', 'win64', 'cd-info.exe')

    # Check if all files and directories exist, and exit if not
    checkDirExists(config.rootDir)
    checkDirExists(config.tempDir)
    checkFileExists(config.prebatchExe)
    checkFileExists(config.loadExe)
    checkFileExists(config.unloadExe)
    checkFileExists(config.rejectExe)
    checkFileExists(config.isoBusterExe)
    checkFileExists(config.dBpowerampConsoleRipExe)
    checkFileExists(config.shntoolExe)
    checkFileExists(config.flacExe)
    checkFileExists(config.cdInfoExe)

    # Check that cdDriveLetter points to an existing optical drive
    resultGetDrives = cdinfo.getDrives()
    cdDrives = resultGetDrives["drives"]
    if config.cdDriveLetter not in cdDrives:
        msg = '"' + config.cdDriveLetter + '" is not a valid optical drive!'
        errorExit(msg)

    # Check that audioFormat is wav or flac
    if config.audioFormat not in ["wav", "flac"]:
        msg = '"' + config.audioFormat + '" is not a valid audio format (expected "wav" or "flac")!'
        errorExit(msg)


def main():
    """Main function"""
    config.version = __version__
    root = tk.Tk()
    myCarrierEntry = carrierEntry(root)
    # This ensures application quits normally if user closes window
    root.protocol('WM_DELETE_WINDOW', myCarrierEntry.on_quit)
    # Start worker as separate thread
    t1 = threading.Thread(target=cdworker.cdWorker, args=[])
    t1.start()

    while True:
        try:
            myCarrierEntry.update_treeview()
            root.update_idletasks()
            root.update()
            time.sleep(0.1)
        except KeyboardInterrupt:
            if config.finishedBatch:
                t1.join()
                handlers = myCarrierEntry.logger.handlers[:]
                for handler in handlers:
                    handler.close()
                    myCarrierEntry.logger.removeHandler(handler)
                # Notify user
                msg = 'Finished processing this batch'
                tkMessageBox.showinfo("Finished", msg)
                # Reset the GUI
                myCarrierEntry.reset_gui()
                # Start cdworker
                root.protocol('WM_DELETE_WINDOW', myCarrierEntry.on_quit)
                t1 = threading.Thread(target=cdworker.cdWorker, args=[])
                t1.start()
            elif config.quitFlag:
                # User pressed exit
                t1.join()
                handlers = myCarrierEntry.logger.handlers[:]
                for handler in handlers:
                    handler.close()
                    myCarrierEntry.logger.removeHandler(handler)
                msg = 'Quitting because user pressed Exit, click OK to exit'
                tkMessageBox.showinfo("Exit", msg)
                os._exit(0)

if __name__ == "__main__":
    main()
