# sftpovw: safer overwrite file over sftp

protect against corruption:

- _old file_: data lost if overwrite fails
- _new file_: partial write may leave corrupt/incomplete file
- _readers_: processes opening the file may observe unspecified/intermediate data

when PUT **localfile** to **remotefile**,

## safe level 0

protect nothing

1. just put **localfile** to **remotefile**

## safe level 1

protect _readers_

1. unlink **remotefile**
2. put **localfile** to **remotefile**

## safe level 2

protect _old file_ and _readers_

1. rename **remotefile** to **tmpfile**
2. put **localfile** to **remotefile**
3. unlink **tmpfile**

## safe level 3

protect _new file_, _old file_, and _readers_

1. put **localfile** to **tmpfile**
2. rename **tmpfile** to **remotefile**

actually rsync supports this level. so most people would use rsync for safe file transfer over ssh.

## safe level 4

protect _new file_, _old file_, and _readers_

usually this level is unnecessary. should only be used if your rename is not atomic.

1. put **localfile** to **tmpfile1**
2. rename **remotefile** to **tmpfile2**
3. rename **tmpfile1** to **remotefile**
4. unlink **tmpfile2**
