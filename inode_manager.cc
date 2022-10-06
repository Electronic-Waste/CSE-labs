#include "inode_manager.h"

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void
disk::read_block(blockid_t id, char *buf)
{
  memcpy(buf, blocks[id], BLOCK_SIZE);
  printf("Read block->src: %s dest: %s\n", blocks[id], buf);
}

void
disk::write_block(blockid_t id, const char *buf)
{
  memcpy(blocks[id], buf, BLOCK_SIZE);
  printf("Write block->src: %s dest: %s\n", buf, blocks[id]);
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  /*
   * your code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.
   */
  blockid_t start = IBLOCK(INODE_NUM, sb.nblocks) + 1;
  for(blockid_t i = start; i < BLOCK_NUM; i++){
    if(using_blocks.count(i) == 0){
      using_blocks[i] = 1;
      return i;
    }
  }
}

void
block_manager::free_block(uint32_t id)
{
  /* 
   * your code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */
  if (using_blocks.find(id) != using_blocks.end()) {
    using_blocks.erase(id);
    using_blocks.insert(std::pair<uint32_t, int>(id, 0));
  }
  return;
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;

}

void
block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void
block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1) {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  /* 
   * your code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
   */
  char buf[BLOCK_SIZE];
  inode_t *ino;
  for(uint32_t inum = 1; inum < INODE_NUM; inum++){
    bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
    ino = (inode_t*)buf + inum%IPB;
    if(ino->type == 0){
      ino->type = type;
      ino->size = 0;
      bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
      return inum;
    }
  }
}

void
inode_manager::free_inode(uint32_t inum)
{
  /* 
   * your code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   */

  return;
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode* 
inode_manager::get_inode(uint32_t inum)
{
  inode_t *ino, *ino_disk;
  char buf[BLOCK_SIZE];

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode *)buf + inum % IPB;
  ino = (inode_t *) malloc(sizeof(inode_t));
  *ino = *ino_disk;
  return ino;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf + inum%IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))

/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_out
   */
  char buf[BLOCK_SIZE];
  char src[BLOCK_SIZE];
  inode_t *ino_disk = get_inode(inum);
  int block_num = ino_disk->size;

  // printf("Block containing inode: %d\n", IBLOCK(inum, bm->sb.nblocks));
  *buf_out = (char *) malloc(block_num * BLOCK_SIZE);
  memset(*buf_out, 0, block_num * BLOCK_SIZE);
  printf("Read_file->Inode size: %d, inum: %d, blockId: %d\n", block_num, inum, ino_disk->blocks[0]);
  for (int i = 0; i < block_num; ++i) {
    bm->read_block(ino_disk->blocks[i], src);
    strncat(*buf_out, src, BLOCK_SIZE);
    *size += strlen(src);
    printf("Strlen: %d\n", strlen(src));
  }
  return;
}

/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf 
   * is larger or smaller than the size of original inode
   */
  char ino_buf[BLOCK_SIZE];
  char dest[BLOCK_SIZE];
  int block_num = size / BLOCK_SIZE + ((size % BLOCK_SIZE == 0) ? 0 : 1);
  struct inode *ino_disk;

  // printf("Block containing inode: %d\n", IBLOCK(inum, bm->sb.nblocks));
  bm->read_block(IBLOCK(inum, bm->sb.nblocks), ino_buf);
  ino_disk = (struct inode *) ino_buf + inum % IPB;
  int cur_block_num = ino_disk->size;
  /* If need to allocate new blocks */
  if (cur_block_num < block_num) {
    for (int i = cur_block_num; i < block_num; ++i) {
      ino_disk->blocks[i] = bm->alloc_block();
    }
    ino_disk->size = block_num;
    put_inode(inum, ino_disk);      // Update corresponding inode
    // extent_protocol::attr a;
    // get_attr(inum, a);
  }
  printf("Write_file->Inum: %d, buf: %s, buf size: %d, current block num: %d, and Inode blocks num: %d\n", inum, buf, size, cur_block_num, ino_disk->size);
  /* Write to file */
  for (int i = 0; i < block_num; ++i) {
    blockid_t blockId = ino_disk->blocks[i];
    uint32_t trans_size = (i != block_num - 1 | size % BLOCK_SIZE == 0) ? BLOCK_SIZE : size % BLOCK_SIZE;
    printf("Transfer size: %d\n", trans_size);
    strncpy(dest, buf + i * BLOCK_SIZE, trans_size);
    if (trans_size != BLOCK_SIZE) // prevent error in writing (possibly write trans_size + 1)
      dest[trans_size] = '\0';
    bm->write_block(blockId, dest);
  }
  
  return;
}

void
inode_manager::get_attr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
  char buf[BLOCK_SIZE];
  inode_t *ino_disk;

  ino_disk = get_inode(inum);
  a.atime = ino_disk->atime;
  a.ctime = ino_disk->ctime;
  a.mtime = ino_disk->mtime;
  a.size = ino_disk->size;
  a.type = ino_disk->type;
  printf("get_attr->inode: %d, size: %d\n", inum, ino_disk->size);
  return;
}

void
inode_manager::remove_file(uint32_t inum)
{
  /*
   * your code goes here
   * note: you need to consider about both the data block and inode of the file
   */
  
  return;
}
