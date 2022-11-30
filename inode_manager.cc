#include "inode_manager.h"
#include <ctime>

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void
disk::read_block(blockid_t id, char *buf)
{
  memcpy(buf, blocks[id], BLOCK_SIZE);
  // printf("Read block->src: %s dest: %s\n", blocks[id], buf);
}

void
disk::write_block(blockid_t id, const char *buf)
{
  memcpy(blocks[id], buf, BLOCK_SIZE);
  // printf("Write block->src: %s dest: %s\n", buf, blocks[id]);
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
    if(using_blocks[i] == 0){
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
  if (using_blocks.count(id) != 0) {
    using_blocks[id] = 0;
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
      // printf("alloc_inode->inum: %d\n", inum);
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
  inode_t *ino_disk = get_inode(inum);
  ino_disk->type = 0;
  ino_disk->size = 0;
  ino_disk->atime = 0;
  ino_disk->mtime = 0;
  ino_disk->ctime = 0;
  for (int i = 0; i <= NDIRECT; ++i)
    ino_disk->blocks[i] = 0;
  
  put_inode(inum, ino_disk);
  delete ino_disk;
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
  /* Initialize some variables */
  char buf[BLOCK_SIZE];
  char src[BLOCK_SIZE];
  blockid_t blockIdList[NDIRECT];
  inode_t *ino_disk = get_inode(inum);
  int block_num = (ino_disk->size + BLOCK_SIZE - 1) / BLOCK_SIZE;

  /* Allocate space for buf_out */
  *buf_out = (char *) malloc(ino_disk->size);
  memset(*buf_out, 0, ino_disk->size);

  // printf("read_file->inum: %d, block_num: %d\n", inum, block_num);

  /* Copy file content to *buf_out */
  /* Case1: The inode does not have indirect block */
  if (block_num <= NDIRECT) {
    for (int i = 0; i < block_num; ++i) {
      bm->read_block(ino_disk->blocks[i], src);
      int trans_size = BLOCK_SIZE;
      if (i == block_num - 1 && ino_disk->size % BLOCK_SIZE != 0)
        trans_size = (ino_disk->size - 1) % BLOCK_SIZE + 1;
      memcpy(*buf_out + i * BLOCK_SIZE, src, trans_size);
    }
  }
  /* Case2: The inode has indirect block */
  else {
    /* NDIRECT blocks in ino->disk */
    for (int i = 0; i < NDIRECT; ++i) {
      bm->read_block(ino_disk->blocks[i], src);
      memcpy(*buf_out + i * BLOCK_SIZE, src, BLOCK_SIZE);
    }
    /* ino->size - NDIRECT blocks in indirect blocks */
    int indirect_block_num = block_num - NDIRECT;
    int indirectId = ino_disk->blocks[NDIRECT];
    get_indirect_block(indirectId, (int *) blockIdList, indirect_block_num);
    for (int i = 0; i < indirect_block_num; ++i) {
      bm->read_block(blockIdList[i], src);
      int trans_size = BLOCK_SIZE;
      if (i == indirect_block_num - 1)
        trans_size = (ino_disk->size - 1) % BLOCK_SIZE + 1;
      memcpy(*buf_out + i * BLOCK_SIZE + NDIRECT * BLOCK_SIZE, src, trans_size);
    }
  }

  *size = ino_disk->size;
  delete ino_disk;
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
  /* Define some variables */
  char ino_buf[BLOCK_SIZE];
  char dest[BLOCK_SIZE];
  blockid_t alloc_blockId[2 * NDIRECT];
  int block_num = (size + BLOCK_SIZE - 1) / BLOCK_SIZE;
  inode_t *ino_disk = get_inode(inum);

  /* Update time */
  std::time_t t = std::time(0);
  ino_disk->atime = t;
  ino_disk->ctime = t;
  ino_disk->mtime = t;

  /* Free blocks in inode: inum*/
  free_blocks_in_inode(inum);

  /* Alloc blocks to store buf */
  for (int i = 0; i < block_num; ++i) {
    alloc_blockId[i] = bm->alloc_block();
    if (i != block_num - 1)
      bm->write_block(alloc_blockId[i], buf + BLOCK_SIZE * i);
    else {
      memset(dest, 0, BLOCK_SIZE);
      memcpy(dest, buf + BLOCK_SIZE * i, (size - 1) % BLOCK_SIZE + 1);
      bm->write_block(alloc_blockId[i], dest);
    }
  }

  /* Update ino->blocks */
  /* Case1: The inode does not have indirect blocks */
  if (block_num <= NDIRECT) {
    for (int i = 0; i < block_num; ++i) {
      ino_disk->blocks[i] = alloc_blockId[i];
    }
  }
  /* Case2: The inode has indirect blocks */
  else {
    for (int i = 0; i < NDIRECT; ++i) {
      ino_disk->blocks[i] = alloc_blockId[i];
    }
    blockid_t blockId = bm->alloc_block();
    ino_disk->blocks[NDIRECT] = blockId;
    write_indirect_block(blockId, (int *) alloc_blockId + NDIRECT, block_num - NDIRECT);
  }
  
  /* Commit changes */
  ino_disk->size = size;
  put_inode(inum, ino_disk);
  delete ino_disk;
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
  // printf("get_attr->inode: %d, size: %d\n", inum, ino_disk->size);
  delete ino_disk;
  return;
}

void
inode_manager::remove_file(uint32_t inum)
{
  /*
   * your code goes here
   * note: you need to consider about both the data block and inode of the file
   */
  free_blocks_in_inode(inum);
  free_inode(inum);
  return;
}

void
inode_manager::get_indirect_block(blockid_t indirectId, int* idList, int size)
{
  char buf[BLOCK_SIZE];
  bm->read_block(indirectId, buf);
  memcpy(idList, buf, sizeof(int) * size);
  // printf("get_indirect_block->indirect block id: %d\n", indirectId);
  // printf("get_indirect_block->indirect buf: %s\n", buf);
  // printf("get_indirect_block->indirect block content: ");
  // for (int i = 0; i < size; ++i)
  //   std::cout << idList[i] << " ";
  // printf("\n");
}

void
inode_manager::write_indirect_block(blockid_t indirectId, int* idList, int size)
{
  char buf[BLOCK_SIZE];
  memcpy(buf, idList, sizeof(int) * size);
  bm->write_block(indirectId, buf);
  // printf("write_indirect_block->indirect block content: ");
  // for (int i = 0; i < size; ++i)
  //   std::cout << idList[i] << " ";
  // printf("\nwrite_indirect_block->indirect buf: %s\n", buf);
}

void
inode_manager::free_blocks_in_inode(uint32_t inum)
{
  inode_t *ino = get_inode(inum);
  int block_num = (ino->size + BLOCK_SIZE - 1) / BLOCK_SIZE;
  if (block_num <= NDIRECT) {
    for (int i = 0; i < block_num; ++i)
      bm->free_block(ino->blocks[i]);
  }
  else {
    for (int i = 0; i < NDIRECT; ++i) 
      bm->free_block(ino->blocks[i]);
    int blockIdList[NDIRECT];
    int indirectId = ino->blocks[NDIRECT];
    get_indirect_block(indirectId, blockIdList, block_num - NDIRECT);
    for (int i = 0; i < block_num - NDIRECT; ++i)
      bm->free_block(blockIdList[i]);
  }
}
