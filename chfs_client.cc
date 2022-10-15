// chfs client.  implements FS operations using extent and lock server
#include "chfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

chfs_client::chfs_client()
{
    ec = new extent_client();

}

chfs_client::chfs_client(std::string extent_dst, std::string lock_dst)
{
    ec = new extent_client();
    if (ec->put(1, "") != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
}

chfs_client::inum
chfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
chfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool
chfs_client::isfile(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        return true;
    } 
    printf("isfile: %lld is a dir\n", inum);
    return false;
}
/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 * 
 * */

bool
chfs_client::isdir(inum inum)
{
    // Oops! is this still correct when you implement symlink?
    return ! isfile(inum);
}

int
chfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    return r;
}

int
chfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    return r;
}


#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

// Only support set size of attr
int
chfs_client::setattr(inum ino, size_t size)
{
    int r = OK;

    /*
     * your code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */
    std::string buf;

    if (ec->get(ino, buf) != extent_protocol::OK) {
        printf("Error: Can't read file (ino %d)\n", ino);
        r = NOENT;
        return r;
    }

    buf.resize(size);

    if (ec->put(ino, buf) != extent_protocol::OK) {
        printf("Error: Can't write file (ino %d)\n", ino);
        r = NOENT;
    }

    return r;
}

int
chfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    std::string buf;
    inum file_inum = 0;
    bool found = false;

    if (ec->get(parent, buf) != extent_protocol::OK) {
        printf("Error: Can't find parent dir %d\n", parent);
        r = IOERR;
        return r;
    }
    
    lookup(parent, name, found, file_inum);
    if (found) {
        printf("Error: file %s already exists\n", name);
        r = EXIST;
        return r;
    }

    if (ec->create(extent_protocol::T_FILE, file_inum) != extent_protocol::OK) {
        printf("Error: Can't create inode for new file\n");
        r = IOERR;
        return r;
    }

    ino_out = file_inum;
    std::string file_name(name);
    buf += file_name + '/' + filename(file_inum) + '/';
    printf("print buf: ");
    for (int i = 0; i < buf.size(); ++i) {
        std::cout << buf[i];
    }
    std::cout << "\n";
    if (ec->put(parent, buf) != extent_protocol::OK) {
        printf("Error: Update for dir in creating new file failed\n");
        r = IOERR;
    }
    printf("creaaaaaaaaaaate file->parent: %d, name: %s, inum: %d\n", parent, name, file_inum);
    return r;
}

int
chfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    std::string buf;
    inum dir_inum = 0;
    bool isFound = false;

    /* Get parent dir content */
    if (ec->get(parent, buf) != extent_protocol::OK) {
        printf("Error: Can't find parent dir %d\n", parent);
        r = IOERR;
        return r;
    }

    /* Check if dir already exists */
    lookup(parent, name, isFound, dir_inum);
    if (isFound) {
        printf("Error: Directory %s already exists\n", name);
        r = EXIST;
        return r;
    }

    /* Create directory */
    if (ec->create(extent_protocol::T_DIR, dir_inum) != extent_protocol::OK) {
        printf("Error: Can't create directory %s\n", name);
        r = IOERR;
        return r;
    } 

    /* Update parent dir's content */
    ino_out = dir_inum;
    std::string dirname(name);
    buf += dirname + '/' + filename(dir_inum) + '/';
    if (ec->put(parent, buf) != extent_protocol::OK) {
        printf("Error: Update for dir in creating new dir failed\n");
        r = IOERR;
    }
    printf("Mkkkkkkkkkkkkkkkkkkdir->parent: %d, name: %s, inum: %d\n", parent, name, dir_inum);

    return r;
}

int
chfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */
    std::string buf;
    int cur_pos;
    int stop_pos;
    int end_pos;

    if (ec->get(parent, buf) != extent_protocol::OK) {
        printf("Error: Can't find parent dir %d\n", parent);
        r = IOERR;
        return r;
    }
    if (!isdir(parent)) {
        printf("Erro: inode %d is not a dir\n", parent);
        r = NOENT;
        return r;
    }

    cur_pos = 0;
    stop_pos = 0;
    end_pos = buf.size();
    while (cur_pos < end_pos) {
        while (buf[stop_pos] != '/') ++stop_pos;
        std::string file_name = buf.substr(cur_pos, stop_pos - cur_pos);
        cur_pos = ++stop_pos;
        // printf("filennnnnnnnnnnnnnnnnnnnname: %s\n", file_name);

        while (buf[stop_pos] != '/') ++stop_pos;
        std::string file_inum = buf.substr(cur_pos, stop_pos - cur_pos);
        cur_pos = ++stop_pos;

        // printf("looooooooooook up in filename: %s, inum: %d. We need: %s\n", file_name, n2i(file_inum), name);
        if (file_name.compare(name) == 0) {
            found = true;
            ino_out = n2i(file_inum);
            // printf("Succeessfullyyyyyyyyyyyyyyyyyyyyyyy lookup->filname: %s, inum: %d\n", file_name, file_inum);
            break;
        }

    }
    // printf("looooooooooooooooooook up!\n");

    return r;
}

int
chfs_client::readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */
    std::string buf;
    int cur_pos;
    int stop_pos;
    int end_pos;

    if (ec->get(dir, buf) != extent_protocol::OK) {
        printf("Error: Can't find parent dir %d\n", dir);
        r = IOERR;
        return r;
    }
    if (!isdir(dir)) {
        printf("Erro: inode %d is not a dir\n", dir);
        r = NOENT;
        return r;
    }

    cur_pos = 0;
    stop_pos = 0;
    end_pos = buf.size();
    while (cur_pos < end_pos) {
        while (buf[stop_pos] != '/') ++stop_pos;
        std::string file_name = buf.substr(cur_pos, stop_pos - cur_pos);
        cur_pos = ++stop_pos;

        while (buf[stop_pos] != '/') ++stop_pos;
        std::string file_inum = buf.substr(cur_pos, stop_pos - cur_pos);
        cur_pos = ++stop_pos;

        struct dirent d;
        d.name = file_name;
        d.inum = n2i(file_inum);
        list.push_back(d);
    }
    // printf("reeeeeeeeeeeeeeeeeeeaddir->\n");
    return r;
}

int
chfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;

    /*
     * your code goes here.
     * note: read using ec->get().
     */
    std::string buf;

    // printf("Reeeeeeeeeeeead inum: %d, size: %d, off: %d\n", ino, size, off);
    if (ec->get(ino, buf) != extent_protocol::OK) {
        printf("Error: Can't read file (ino %d)\n", ino);
        r = NOENT;
        return r;
    }

    // printf("buf: %s buf size: %d\n", buf.c_str(), buf.size());
    int buf_size = buf.size();
    data = buf.substr(off, size);
    // printf("Reeeeeeeeeeed-> data: %s data size: %d\n", data.c_str(), data.size());

    return r;
}

int
chfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    int r = OK;

    /*
     * your code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */
    std::string buf;

    // printf("Wriiiiiiiiiiiiiiiiite: inum: %d, size: %d, offset: %d, data: %s\n", ino, size, off, data);
    if (ec->get(ino, buf) != extent_protocol::OK) {
        printf("Error: Can't read file (ino %d)\n", ino);
        r = NOENT;
        return r;
    }
    if (buf.size() < off) {
        buf.resize(off, '\0');
    }
    buf.replace(off, size, data, size);
    bytes_written += size;
    if (ec->put(ino, buf) != extent_protocol::OK) {
        printf("Error: Can't write back to files (inum: %d)\n", ino);
        r = NOENT;
    }

    return r;
}

int chfs_client::unlink(inum parent,const char *name)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */
    std::string buf;
    inum file_inum = 0;
    bool isFound = false;

    /* Get parent dir content */
    if (ec->get(parent, buf) != extent_protocol::OK) {
        printf("Error: Can't find parent dir %d\n", parent);
        r = IOERR;
        return r;
    }
    printf("Unnnnnnnnnnnnnnnnnnnnnnnnlink-> prev buf: %s\n", buf.c_str());

    /* Check if the file already exists */
    lookup(parent, name, isFound, file_inum);
    if (!isFound) {
        printf("Error: Can't find file %s\n", name);
        r = IOERR;
        return r;
    }

    /* Delete file */
    if (ec->remove(file_inum) != extent_protocol::OK) {
        printf("Error: Can't delete file: %s\n", name);
        r = IOERR;
        return r;
    }

    /* Update parent dir */
    int cur_pos = 0;
    int stop_pos = 0;
    int end_pos = buf.size();
    while (cur_pos < end_pos) {
        while (buf[stop_pos] != '/') ++stop_pos;
        std::string file_name = buf.substr(cur_pos, stop_pos - cur_pos);
        ++stop_pos;
        /* Filename matched: erase 'filename/inum/' */
        if (file_name.compare(name) == 0) {
            while (buf[stop_pos] != '/') ++stop_pos;
            buf.erase(cur_pos, stop_pos - cur_pos + 1);
            break;
        }
        /* Else: Skip inum and move forward */
        else {
            while (buf[stop_pos] != '/') ++stop_pos;
            cur_pos = ++stop_pos;
        }
    }
    if (ec->put(parent, buf) != extent_protocol::OK) {
        printf("Error: Can't update parent dir content!\n");
        r = IOERR;
    }
    printf("Unnnnnnnnnnnnnnnnnnlink-> next buf: %s\n", buf.c_str());

    return r;
}

