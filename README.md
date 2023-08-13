# diskhash
on-disk hash table index(mainly for WAL).

## When you will need it?
if you are using [WAL](https://github.com/rosedblabs/wal) to store your data,

> wal: https://github.com/rosedblabs/wal

you will get the positions to get the data from WAL, the common way to store the positions is to use an in-memory index, but if you have a large amount of data, the index will be very large, and it will take a lot of time to load the index into memory when you restart the program.

so, you can use diskhash to store the index on disk.

## Can be used as a general hash table index(without wal)?

yes, you can use it as an on-disk hash table index, but the restriction is that the value must be fixed size.
you can set the value size when you create the index, and once you set the value size, you can't change it.

But don't set the value size too large(1KB), the disk size maybe increase dramatically because of the write amplification.
**it is suitable for storing some metadata of your system.**
