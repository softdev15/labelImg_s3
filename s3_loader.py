import math
from urllib.parse import urlparse
import getpass
import sys
import string
from functools import partial, partialmethod
import pathlib
import boto3
import psycopg2
import appdirs
import os
import shutil
from libs.pascal_voc_io import PascalVocReader, PascalVocWriter
try:
    from PyQt5.QtGui import *
    from PyQt5.QtCore import *
    from PyQt5.QtWidgets import *
except ImportError:
    # needed for py3+qt4
    # Ref:
    # http://pyqt.sourceforge.net/Docs/PyQt4/incompatible_apis.html
    # http://stackoverflow.com/questions/21217399/pyqt4-qtcore-qvariant-object-instead-of-a-string
    if sys.version_info.major >= 3:
        import sip
        sip.setapi('QVariant', 2)
    from PyQt4.QtGui import *
    from PyQt4.QtCore import *



class S3Loader():
    def __init__(self, window: QMainWindow, acceptable_extensions=[], should_lock_files=False, release_lock_individually=True, auto_loading_dir=None):

        self.automatically_load_data_from = None
        self.window = window
        self.acceptable_extensions = acceptable_extensions 
        self.should_lock_files = should_lock_files
        self.release_lock_individually = release_lock_individually
        self.owned_locks = []
        self.delete_on_close = False

        if urlparse(auto_loading_dir).scheme == "s3":
                self.automatically_load_data_from = auto_loading_dir

        self.save_dir = os.path.join(appdirs.user_data_dir(), "shipamax.labelImg", acceptable_extensions[0])

        self.reset()
        self.s3 = boto3.client("s3")


        vlayout = QVBoxLayout()
        vlayout.setContentsMargins(0, 0, 0, 0)


        self.picker_widget = QWidget()
        self.picker_widget.setLayout(vlayout)
        self.picker_widget.setGeometry(QRect(10, 10, 300, 500))
        self.picker_widget.windowModality = Qt.ApplicationModal

        self.to_download = []
        
        self.file_list_widget = QListWidget()
        button_widget = QWidget()

        vlayout.addWidget(self.file_list_widget)
        vlayout.addWidget(button_widget)
        hlayout = QHBoxLayout()
        button_widget.setLayout(hlayout)


        back_button = QToolButton()
        back_button.setToolButtonStyle(Qt.ToolButtonTextOnly)
        self.back_action = QAction("back")
        self.back_action.setEnabled(False)
        back_button.setDefaultAction(self.back_action)
        
        confirm_button = QToolButton()

        confirm_button.setToolButtonStyle(Qt.ToolButtonTextOnly)
        self.confirm_action = QAction("confirm")
        self.confirm_action.setEnabled(False)
        confirm_button.setDefaultAction(self.confirm_action)

        self.back_action.triggered.connect(self.back_clicked)
        self.confirm_action.triggered.connect(self.confirm_clicked)
        #new_action(button_widget, text="Back", slot=self.back_clicked)

        hlayout.addWidget(back_button)
        hlayout.addWidget(confirm_button)

        #self.file_list_widget.itemSelectionChanged.connect(self.s3_file_selected)
        self.file_list_widget.itemDoubleClicked.connect(self.selection_double_clicked)
        try:
            self.con = psycopg2.connect(
                host=os.environ["PG_HOST"],
                port=os.environ["PG_PORT"],
                database=os.environ["PG_DATABASE"],
                user=os.environ["PG_USERNAME"],
                password=os.environ["PG_PASSWORD"]
            )
            self.cursor = self.con.cursor()
        except Exception as e:
            print(e)
            print("Could not connect to database")
            sys.exit(1)
        if not self.should_lock_files:
            self.window.load_file = partial(self.load_image_patch, self.window.load_file)
            self.window.last_open_dir = self.save_dir
        else:
            PascalVocReader._parse_xml = PascalVocReader.parse_xml
            PascalVocReader.parse_xml = partialmethod(load_xml_patch, self)
            PascalVocWriter._save = PascalVocWriter.save
            PascalVocWriter.save = partialmethod(save_xml_patch, self)
            self.window.closeEvent = partial(self.close_event_patch, self.window.closeEvent)
            self.window.default_save_dir = self.save_dir
            if self.automatically_load_data_from:
                parsed_path = urlparse(self.automatically_load_data_from)
                self.selected_bucket = parsed_path.netloc
                self.current_path = parsed_path.path
                self.s3_file_selected()
                self.automatically_load_data_from = None
                self.confirm_clicked(skip_ui_update=True)

    def confirm_clicked(self, skip_ui_update=False):
        existing_files = self.current_dir_files
        selected_files = [existing_files[i.row()] for i in self.file_list_widget.selectedIndexes()]
        
        if len(selected_files) == 0:
            selected_files = existing_files
        
        if os.path.exists(self.save_dir) and os.path.isdir(self.save_dir):
            shutil.rmtree(self.save_dir)
        os.makedirs(self.save_dir, exist_ok=True)
        self.to_download = selected_files
        self.picker_widget.close()
        for i,f in enumerate(self.to_download):
            name = self.getfilename(f)
            dest_path = os.path.join(self.save_dir,name)
            open(dest_path, "w").close()

        if not self.should_lock_files and not skip_ui_update:
            self.window.import_dir_images(self.save_dir)
        elif not skip_ui_update and self.window.file_path:
            self.window.show_bounding_box_from_annotation_file(self.window.file_path)

    def back_clicked(self):
        print(f"WAS : {self.current_path}")
        if self.current_path and self.current_path != "/":
            self.current_path = "/".join(self.current_path.split("/")[:-2])+"/"
            print(f"IS: {self.current_path}")
            self.s3_file_selected()
        elif self.selected_bucket:
            self.reset()
            self.list_buckets()
            self.back_action.setEnabled(False)
            self.confirm_action.setEnabled(False)
        print("back")
    
    def list_buckets(self):
        buckets = self.s3.list_buckets()
        self.file_list_widget.clear()
        self.file_list_widget.addItems([x["Name"] for x in buckets["Buckets"]])
    def open_bucket_view(self):
        self.reset()
        self.list_buckets()
        self.picker_widget.show()
    
    def selection_double_clicked(self):
        if self.selected_bucket is None and len(self.file_list_widget.selectedItems()) == 1:
            self.selected_bucket = self.file_list_widget.selectedItems()[0].text()
        elif self.selected_bucket and len(self.file_list_widget.selectedItems()) == 1:
            self.current_path = os.path.join(
                self.current_path,
                self.file_list_widget.selectedItems()[0].text()
            )
            if self.current_path[0] != "/":
                self.current_path = "/" + self.current_path
        self.s3_file_selected()

    def s3_file_selected(self, item=None):

        if self.selected_bucket:
            self.back_action.setEnabled(True)
            paginator = self.s3.get_paginator('list_objects')
            page_iterator = paginator.paginate(Bucket=self.selected_bucket, Prefix=f'{self.current_path[1:]}', Delimiter='/')

            # remove all existing items
            self.file_list_widget.clear()
            folders = []
            raw_files = []
            for page in page_iterator:
                
                for folder in page.get('CommonPrefixes', []):
                    if folder is None:
                        continue
                    folders.append("/".join(folder.get('Prefix').strip().split("/")[-2:])) 
                
                for file in page.get('Contents', []):
                    if file is None or file.get('Key').strip()[-1] == "/" or pathlib.Path(file.get('Key')).suffix.strip(string.punctuation) not in self.acceptable_extensions:
                        continue
                    raw_files.append(file.get('Key'))

            files = [self.getfilename(x) for x in raw_files]
            self.current_dir_files = raw_files
            self.file_list_widget.addItems(folders+files)
            self.file_list_widget.setSelectionMode(
                QAbstractItemView.ExtendedSelection
            )
            self.selection_changed()
    
    def selection_changed(self):
        selected_files = [x.text() for x in self.file_list_widget.selectedItems()]
        if len(selected_files) == 0 and len(self.current_dir_files) != 0:
            self.confirm_action.setEnabled(True)
        else:
            self.confirm_action.setEnabled(False)


    def reset(self):
        self.selected_bucket = None
        self.current_path = ""
        self.current_dir_files = []
        self.to_download = []
        self.owned_locks = []


    def download_file(self, file):
        results = None
        file_to_release = None
        if self.release_lock_individually and self.owned_locks:
                file_to_release = self.owned_locks[0]
               
        if self.should_lock_files:
            self.execute("SELECT * from annotation_locks where file = %s",(file,))
            results = self.cursor.fetchall()
        if results:
            QMessageBox.critical(self.window,"Error", "The file you're trying to open is currently locked. Annotation changes will only be saved locally unless you own the lock.")
        elif self.should_lock_files:
            
            self.execute("INSERT INTO annotation_locks (file, owner) VALUES (%s, %s)", (file, getpass.getuser()))
            self.owned_locks.append(file)
        
        if file_to_release != file and file_to_release is not None:
            self.execute("DELETE FROM annotation_locks where file = %s and owner = %s", (file_to_release, getpass.getuser()))
            self.owned_locks.remove(file_to_release)
        self.con.commit()
        dest_path = os.path.join(self.save_dir,self.getfilename(file))
        self.s3.download_file(self.selected_bucket,file, dest_path) 

    def execute(self, query, params=None):
        try:
            return self.cursor.execute(query, params)
        except (psycopg2.DataError, psycopg2.InternalError) as e:
            self.logger.error(e)
            self.logger.info('Ending current transaction after error')
            self.con.execute("END TRANSACTION;")
            raise e

    def download_remote_file(self, file_path):
        name = self.getfilename(file_path)
        remote_file = [x for x in self.to_download if self.getfilename(x) == name][0]
        self.download_file(remote_file)

    def load_image_patch(self, prev_call, file_path=None):
        if self.automatically_load_data_from:
            parsed_path = urlparse(self.automatically_load_data_from)
            self.selected_bucket = parsed_path.netloc
            self.current_path = parsed_path.path
            self.s3_file_selected()
            self.automatically_load_data_from = None
            self.confirm_clicked()
        else:
            self.download_remote_file(file_path)
            prev_call(file_path)

    def close_event_patch(self, prev_call, event):
        
        self.delete_on_close = True
        
        prev_call(event)

        self.delete_on_close = self.delete_on_close and (len(self.owned_locks) > 0) 
        if self.delete_on_close and self.owned_locks:
            ret = QMessageBox.question(self.window,'', "Should I release currently held locks ", QMessageBox.Yes | QMessageBox.No)
            self.delete_on_close = self.delete_on_close and (ret == QMessageBox.Yes) 
        if self.delete_on_close:
            for f in self.owned_locks:
                self.execute("DELETE FROM annotation_locks where file = %s and owner = %s", (f, getpass.getuser()))
            self.con.commit()
            self.owned_locks = []
            self.delete_on_close = False

    def getfilename(self,file):
        return file.split("/")[-1]

def save_xml_patch(self, s3_loader, target_file=None):
    filename = s3_loader.getfilename(target_file)
    self._save(target_file)
    full_filename = [x for x in s3_loader.to_download if filename in x]
    results = None
    if full_filename:
        full_filename = full_filename[0]
        try:
            s3_loader.execute("SELECT * from annotation_locks where file = %s",(full_filename,))
            results = s3_loader.cursor.fetchall()
        except:
            print("Could not connect to database")
    should_upload = False
    if not results and filename not in s3_loader.owned_locks:
        QMessageBox.critical(s3_loader.window, "Error", "You're trying to save a file which is not currently locked.\n The file will be saved locally but not uploaded.\n If you want to annotate it, you'll have to reopen it to make sure you own the lock.")
    elif results and results[0][0] != getpass.getuser():
        QMessageBox.critical(s3_loader.window, "Error", "You're trying to save a file which is currently locked by another user.\n The file will be saved locally but not uploaded.\n If you want to annotate it, you'll have to reopen it after it is unlocked.")
    elif results and results[0][0] == getpass.getuser() and filename not in s3_loader.owned_locks:
        ret = QMessageBox.question(s3_loader.window, "", "You're trying to save a file which is currently locked by you, but whose lock was not created in this session. Do you want to upload the result and release the lock?")
        should_upload = QMessageBox.Yes == ret
    else:
        should_upload = True
    if should_upload:
        with open(target_file, "rb") as f:
            s3_loader.s3.upload_fileobj(f, s3_loader.selected_bucket, full_filename)
        self.delete_on_close = False
        s3_loader.execute("DELETE FROM annotation_locks where file = %s and owner = %s", (full_filename, getpass.getuser()))
        if full_filename in s3_loader.owned_locks:
            s3_loader.owned_locks.remove(full_filename)
        s3_loader.con.commit()

def load_xml_patch(self, s3_loader):
    s3_loader.download_remote_file(self.file_path)
    self._parse_xml()
