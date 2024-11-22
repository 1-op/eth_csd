package main

import (
	trie "awesomeProject1/Prefix_MPT"
	"awesomeProject1/ethdb"
	"awesomeProject1/goleveldb/leveldb/cache"
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"io"
	log2 "log"
	"math/big"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"time"
)

var (
	// databaseVerisionKey tracks the current database version.
	databaseVersionKey = []byte("DatabaseVersion")

	// headHeaderKey tracks the latest known header's hash.
	headHeaderKey = []byte("LastHeader")

	// headBlockKey tracks the latest known full block's hash.
	headBlockKey = []byte("LastBlock")

	// headFastBlockKey tracks the latest known incomplete block's hash during fast sync.
	headFastBlockKey = []byte("LastFast")

	// fastTrieProgressKey tracks the number of trie entries imported during fast sync.
	fastTrieProgressKey = []byte("TrieSync")

	// snapshotRootKey tracks the hash of the last snapshot.
	snapshotRootKey = []byte("SnapshotRoot")

	// snapshotJournalKey tracks the in-memory diff layers across restarts.
	snapshotJournalKey = []byte("SnapshotJournal")

	// txIndexTailKey tracks the oldest block whose transactions have been indexed.
	txIndexTailKey = []byte("TransactionIndexTail")

	// fastTxLookupLimitKey tracks the transaction lookup limit during fast sync.
	fastTxLookupLimitKey = []byte("FastTransactionLookupLimit")

	// Data item prefixes (use single byte to avoid mixing data types, avoid `i`, used for indexes).
	headerPrefix       = []byte("h") // headerPrefix + num (uint64 big endian) + hash -> header
	headerTDSuffix     = []byte("t") // headerPrefix + num (uint64 big endian) + hash + headerTDSuffix -> td
	headerHashSuffix   = []byte("n") // headerPrefix + num (uint64 big endian) + headerHashSuffix -> hash
	headerNumberPrefix = []byte("H") // headerNumberPrefix + hash -> num (uint64 big endian)

	blockBodyPrefix     = []byte("b") // blockBodyPrefix + num (uint64 big endian) + hash -> block body
	blockReceiptsPrefix = []byte("r") // blockReceiptsPrefix + num (uint64 big endian) + hash -> block receipts

	txLookupPrefix        = []byte("l") // txLookupPrefix + hash -> transaction/receipt lookup metadata
	bloomBitsPrefix       = []byte("B") // bloomBitsPrefix + bit (uint16 big endian) + section (uint64 big endian) + hash -> bloom bits
	SnapshotAccountPrefix = []byte("a") // SnapshotAccountPrefix + account hash -> account trie value
	SnapshotStoragePrefix = []byte("o") // SnapshotStoragePrefix + account hash + storage hash -> storage trie value

	preimagePrefix = []byte("secure-key-")      // preimagePrefix + hash -> preimage
	configPrefix   = []byte("ethereum-config-") // config prefix for the db

	// Chain index prefixes (use `i` + single byte to avoid mixing data types).
	BloomBitsIndexPrefix = []byte("iB") // BloomBitsIndexPrefix is the data table of a chain indexer to track its progress

	Count int
	tt1   float64
	tt2   float64
	tt3   float64
	tx    []byte
	ac    []byte
	key   []byte
	value []byte
)

func encodeBlockNumber(number uint64) []byte {
	enc := make([]byte, 8)
	binary.BigEndian.PutUint64(enc, number)
	return enc
}

func headerKey(hash common.Hash, number uint64) []byte {
	return append(append(headerPrefix, encodeBlockNumber(number)...), hash.Bytes()...)
}

func GetHeaderRLP(db *myethdb.LDBDatabase, hash common.Hash, number uint64) rlp.RawValue {
	data, _ := db.Get(headerKey(hash, number))
	return data
}
func GetHeader(db *myethdb.LDBDatabase, hash common.Hash, number uint64) *types.Header {
	data := GetHeaderRLP(db, hash, number)
	if len(data) == 0 {
		return nil
	}
	header := new(types.Header)
	if err := rlp.Decode(bytes.NewReader(data), header); err != nil {
		fmt.Println("Error")
		return nil
	}
	return header
}

// ReadDatabaseVersion retrieves the version number of the database.
func ReadDatabaseVersion(db *myethdb.LDBDatabase) *uint64 {
	var version uint64

	enc, _ := db.Get(databaseVersionKey)
	if len(enc) == 0 {
		return nil
	}
	if err := rlp.DecodeBytes(enc, &version); err != nil {
		return nil
	}

	return &version
}

func ReadTxLookupEntry(db *myethdb.LDBDatabase, hash common.Hash) uint64 {
	// Load the positional metadata from disk and bail if it fails
	data, _ := db.Get(append(txLookupPrefix, hash.Bytes()...))
	if len(data) == 0 {
		return 0
	}
	if len(data) < common.HashLength {
		number := new(big.Int).SetBytes(data).Uint64()
		return number
	}
	return 0
}

// headerHashKey = headerPrefix + num (uint64 big endian) + headerHashSuffix
func headerHashKey(number uint64) []byte {
	return append(append(headerPrefix, encodeBlockNumber(number)...), headerHashSuffix...)
}

func blockBodyKey(hash common.Hash, number uint64) []byte {
	return append(append(blockBodyPrefix, encodeBlockNumber(number)...), hash.Bytes()...)
}

// ReadBodyRLP retrieves the block body (transactions and uncles) in RLP encoding.
func ReadBodyRLP(db *myethdb.LDBDatabase, hash common.Hash, number uint64) rlp.RawValue {
	// First try to look up the data in ancient database. Extra hash
	// comparison is necessary since ancient database only maintains
	// the canonical data.
	var data []byte
	data, _ = db.Get(blockBodyKey(hash, number))
	return data
}

// ReadBody retrieves the block body corresponding to the hash.
func ReadBody(db *myethdb.LDBDatabase, hash common.Hash, number uint64) *types.Body {
	data := ReadBodyRLP(db, hash, number)
	if len(data) == 0 {
		return nil
	}
	body := new(types.Body)
	if err := rlp.Decode(bytes.NewReader(data), body); err != nil {
		log.Error("Invalid block body RLP", "hash", hash, "err", err)
		return nil
	}
	return body
}

func GetTransaction(db *myethdb.LDBDatabase, hash common.Hash) (*types.Transaction, common.Hash, uint64, uint64) {
	// Retrieve the lookup metadata and resolve the transaction from the body
	// 取出区块号
	t1 := time.Now()
	blockNumber := ReadTxLookupEntry(db, hash)
	t2 := time.Now()
	tt1 += t2.Sub(t1).Seconds()
	//fmt.Println("blocknumber:",blockNumber)

	t3 := time.Now()
	headerhash := headerHashKey(blockNumber) // prefix + num + suffix --> hash
	// 取区块hash
	blkhash3, _ := db.Get(headerhash)
	t4 := time.Now()
	tt2 += t4.Sub(t3).Seconds()

	body := ReadBody(db, common.BytesToHash(blkhash3), blockNumber) // b + num + hash --> body

	t5 := time.Now()
	tt3 += t5.Sub(t4).Seconds()

	if body == nil {
		Count++
		log.Error("Transaction referenced missing", "number", blockNumber, "hash", blkhash3)
		return nil, common.Hash{}, 0, 0
	}
	for txIndex, tx := range body.Transactions {
		if tx.Hash() == hash {
			return tx, common.BytesToHash(blkhash3), blockNumber, uint64(txIndex)
		}
	}
	//log.Error("Transaction not found", "number", blockNumber, "hash", blkhash3, "txhash", hash)
	return nil, common.Hash{}, 0, 0
}

func TestObtainAllMPTroot(db *myethdb.LDBDatabase) {

	// record all mpt root
	fr, err2 := os.OpenFile("mpt_root", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0660)
	if err2 != nil {
		fmt.Println("Error")
	}
	defer fr.Close()

	version := ReadDatabaseVersion(db)
	fmt.Printf("Version %d\n", *version)

	header_nil_cnt := 0
	body_nil_cnt := 0
	number := 0 //从200万开始
	blockheight := 21199670
	for i := number; i < blockheight; i++ {
		hashkey := append(append(headerPrefix, encodeBlockNumber(uint64(i))...), headerHashSuffix...)

		// qukuaide hash
		hash, _ := db.Get(hashkey)
		header := GetHeader(db, common.BytesToHash(hash), uint64(i))
		if header == nil {
			header_nil_cnt++
			continue
		}
		body := ReadBody(db, common.BytesToHash(hash), uint64(i))
		if body == nil {
			body_nil_cnt++
			continue
		}
		MPTroot := header.Root.Bytes()

		fmt.Println("block number : " + strconv.FormatInt(header.Number.Int64(), 10))
		fmt.Println("block hash : " + hex.EncodeToString(hash))
		fmt.Println("parent hash : " + hex.EncodeToString(header.ParentHash.Bytes()))
		fmt.Println("tx hash : " + hex.EncodeToString(header.TxHash.Bytes()))
		fmt.Println("state hash : " + hex.EncodeToString(MPTroot))

		mpt, _ := db.Get(MPTroot)
		if mpt != nil {
			fmt.Println("mpt : " + hex.EncodeToString(mpt))
			fmt.Fprintln(fr, hex.EncodeToString(MPTroot))

		}
		if header.Number.Int64() == 2 {
			//break
		}
		//fmt.Fprintln(fr, hex.EncodeToString(MPTroot))
	}
	fmt.Printf("header nil count : %d", header_nil_cnt)
	fmt.Printf("body nil count : %d", header_nil_cnt)

}
func write_txt_kv_2_db(db *myethdb.LDBDatabase) {

	defer db.Close()
	fi, err := os.Open("E:\\DB\\ExpData-80G")
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}
	//defer db2.Close()Double
	defer fi.Close()
	size := 0
	size32 := 0
	sizeNo32 := 0
	totali := 0
	keyi := 0
	br := bufio.NewReader(fi)
	buf := []byte{0, 0, 0, 0}

	cnt_l := 0
	cnt_h := 0
	cnt_t := 0
	cnt_n := 0
	cnt_H := 0
	cnt_b := 0
	cnt_r := 0
	cnt_B := 0
	cnt_a := 0
	cnt_o := 0
	for {
		if totali%2 == 0 {
			a, c := br.ReadString('\n')
			key, _ = hex.DecodeString(a)
			keyi++
			if c == io.EOF {
				break
			}
		} else {
			a, c := br.ReadString('\n')
			value, _ = hex.DecodeString(a)
			//size += len(value)
			if c == io.EOF {
				break
			}

			if len(key) == 32 {
				// state data
				key = append(buf[:], key...)
				//_ = db.Put(key, value)
				size32 += len(key) + len(value)
			} else {

				if len(key) >= 33 && bytes.Compare(key[:1], txLookupPrefix) == 0 {
					cnt_l++
				} else if len(key) >= 33 && bytes.Compare(key[:1], headerPrefix) == 0 {
					cnt_h++
				} else if len(key) >= 33 && bytes.Compare(key[:1], headerTDSuffix) == 0 {
					cnt_t++
				} else if len(key) >= 33 && bytes.Compare(key[:1], headerHashSuffix) == 0 {
					cnt_n++
				} else if len(key) >= 33 && bytes.Compare(key[:1], headerNumberPrefix) == 0 {
					cnt_H++
				} else if len(key) >= 33 && bytes.Compare(key[:1], blockBodyPrefix) == 0 {
					cnt_b++
				} else if len(key) >= 33 && bytes.Compare(key[:1], blockReceiptsPrefix) == 0 {
					cnt_r++
				} else if len(key) >= 33 && bytes.Compare(key[:1], bloomBitsPrefix) == 0 {
					cnt_B++
				} else if len(key) >= 33 && bytes.Compare(key[:1], SnapshotAccountPrefix) == 0 {
					cnt_a++
				} else if len(key) >= 33 && bytes.Compare(key[:1], SnapshotStoragePrefix) == 0 {
					cnt_o++
				}

				//if len(key) == 33 && bytes.Compare(key[:1], []byte("l")) == 0 {
				//	// the kv belongs to lookup tx entry
				//	//buf = []byte{0,0,0}
				//	//temp = append([]byte{0},buf[:]...)
				//	//key = append(temp,key...)
				//	//_ = db.Put_s(key, value)
				//	sizeNo32 += len(key) + len(value)
				//} else if len(key) == 33 && bytes.Compare(key[:1], []byte("H")) == 0 {
				//	buf = value[4:]
				//	fmt.Printf("%d\n", buf)
				//	key = append(buf[:], key...)
				//	//_ = db.Put(key, value)
				//	sizeNo32 += len(key) + len(value)
				//} else {
				//
				//	// else non-state data
				//	key = append(buf[:], key...)
				//	_ = db.Put(key, value)
				//	sizeNo32 += len(key) + len(value)
				//}
			}
		}
		totali++
	}
	fmt.Printf("l:%d h:%d t:%d n:%d H:%d b:%d r:%d B:%d o:%d\n", cnt_l, cnt_h, cnt_t, cnt_n, cnt_H, cnt_b, cnt_r, cnt_B, cnt_a, cnt_o)

	size = size32 + sizeNo32
	fmt.Println("总条目数:", totali, "key数目:", keyi)
	fmt.Println("总大小:", size, size32, sizeNo32, size32+sizeNo32)
}
func BytesToHash(b []byte) common.Hash {
	var h common.Hash
	h.SetBytes(b)
	return h
}

var (
	Txhash  [11000000][]byte //长度代表你想要读取的交易的set数目
	Acchash [11000000][]byte
	LoopCnt int //循环次数
)

func TestTranscation(db *myethdb.LDBDatabase) {
	f1, _ := os.Open("E:\\DB\\Tx.txt")

	s1 := bufio.NewScanner(f1)
	Txnumber := 0 // 记录txt中当前遍历到的tx的数目
	Count_T := 0  // 记录当前持有的tx的数目
	for s1.Scan() {
		str := s1.Text()
		key, _ := hex.DecodeString(str[:])
		// 插入MPT
		if Txnumber%19 == 0 {
			Txhash[Count_T] = key
			Count_T++
		}
		Txnumber++
		if Count_T == 10000000 { //保存了10100000个交易
			break
		}
	}
	_ = f1.Close()
	fmt.Println("============================")
	number := 0
	rand.Seed(0)
	LoopCnt = 100000
	for i := 0; i < LoopCnt; i++ {
		//j := rand.Intn(10000000)
		//j:=5284409
		j := i
		tx = Txhash[j]
		txh := BytesToHash(tx)
		//_, _, _, _ = GetTransaction_s(*db, txh)
		_, _, _, _ = GetTransaction(db, txh)
		//fmt.Println(tt1, tt2, tt3)

		number++
		if i%1000000 == 0 {
			log2.Println(i)
		}
	}
	fmt.Println(tt1, tt2, tt3)
	tt := tt1 + tt2 + tt3
	fmt.Println(Count)
	fmt.Println(tt)
	runtime.GC()
}

// ID is the identifier for uniquely identifying a trie.
type ID struct {
	StateRoot common.Hash // The root of the corresponding state(block.root)
	Owner     common.Hash // The contract address hash which the trie belongs to
	Root      common.Hash // The root hash of trie
}

func TestAcc(db *myethdb.LDBDatabase) {
	//root:=[]byte{239, 104, 4, 219, 125, 134, 93, 238, 147, 175, 67, 122, 141, 12, 252, 148, 160, 72, 197, 46, 81, 57, 245, 6, 212, 190, 167, 146, 180, 95, 154, 228}
	//root:=[]byte{240, 114, 194, 194, 17, 14, 199, 231, 50, 103, 168, 144, 117, 47, 201, 67, 245, 137, 219, 7, 254, 234, 2, 157, 3, 151, 148, 51, 109, 16, 189, 157, 145}
	//root := []byte{0xab, 0x25, 0xcc, 0x66, 0x84, 0xa5, 0x13, 0x75, 0x87, 0x79, 0xd0, 0x8a, 0x5a, 0x18, 0xd6, 0xb1, 0xf6, 0x77, 0x3c, 0x41, 0x89, 0x1f, 0x90, 0xa3, 0x60, 0x22, 0xdc, 0x65, 0xa9, 0x4e, 0xba, 0x57}
	root := []byte{0x4a, 0x34, 0xec, 0xf2, 0xfa, 0x53, 0xfa, 0x88, 0xf9, 0xfd, 0xda, 0x91, 0x30, 0x65, 0xc4, 0x43, 0xc8, 0xd0, 0x1c, 0x47, 0xfa, 0x0a, 0x73, 0xe1, 0x2b, 0xc4, 0xb4, 0x2c, 0x6f, 0x9f, 0x61, 0x59}
	tree, _ := trie.News(root, db)
	//return
	f, _ := os.Open("E:\\DB\\UniqueA2.txt")
	s := bufio.NewScanner(f)
	Count_A := 0
	//474022
	var shijian float64
	for s.Scan() {
		str := s.Text()
		key, _ := hex.DecodeString(str[:])
		// 插入MPT
		Acchash[Count_A] = key
		Count_A++
		if Count_A == 10100000 {
			break
		}
	}
	_ = f.Close()
	number := 0
	t1 := time.Now()
	rand.Seed(0)
	const num = 0
	for i := 0; i < num; i++ {
		j := rand.Intn(10000000)
		j = i
		ac := Acchash[j]
		//tree, _ := News(root, db)
		v := tree.Get(crypto.Keccak256(ac))
		if v == nil {
			Count++
		}
		if v != nil {
			//fmt.Println(j, string(v))
		}
		if i%100000 == 0 {
			log2.Println(i)
		}
		number++
	}
	t2 := time.Now()
	shijian += t2.Sub(t1).Seconds()
	//TimeTx := tt1 + tt2 + tt3
	fmt.Println("总时间耗费：", shijian)
	fmt.Println("nil计数", Count, number)
	//fmt.Println("kv数目,总时间，交易时间", db.Count, ethdb.T, TimeTx, shijian)
	//fmt.Println("qps:", float64(num)/ethdb.T)
	fmt.Println(cache.HitNumber, cache.MissNumber, float64(cache.HitNumber)/float64(cache.HitNumber+cache.MissNumber))
	//fmt.Println(cache.HitNumber2, cache.MissNumber2, float64(cache.HitNumber2)/float64(cache.HitNumber2+cache.MissNumber2))
	//fmt.Println("命中率", float64(cache.HitNumber+cache.HitNumber2)/float64(cache.HitNumber2+cache.MissNumber2+cache.HitNumber+cache.MissNumber))

	runtime.GC()
}

func main() {
	//path := "E:\\eth\\execution\\db\\geth\\chaindata"
	path := "E:\\DB\\db\\10G"
	db, err := myethdb.NewLDBDatabase(path, 16, 128)
	if err != nil {
		return
	}
	defer db.LDB()

	//h := []byte{0xea, 0x10, 0x93, 0xd4, 0x92, 0xa1, 0xdc, 0xb1, 0xbe, 0xf7, 0x08, 0xf7, 0x71, 0xa9, 0x9a, 0x96, 0xff, 0x05, 0xdc, 0xab, 0x81, 0xca, 0x76, 0xc3, 0x19, 0x40, 0x30, 0x01, 0x77, 0xfc, 0xf4, 0x9f}
	//_, _, a, b := GetTransaction(db, BytesToHash(h))
	//fmt.Printf("tx block : %d\n", a)
	//fmt.Printf("tx index : %d\n", b)
	//fmt.Println(tt1, tt2, tt3)

	//write_txt_kv_2_db(db)

	TestTranscation(db)

	//TestObtainAllMPTroot(db)

	//TestAcc(db)

}
