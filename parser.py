# -------------------------------------------------------------
#
# This script is a modified version of Blockchain parser by Denis Leonov.
# We are using the .blk file parsing to extract each transaction RawTX and
# retrieve required information using the btcpy library, to generate the output files.
#
# Blockchain parser: https://github.com/ragestack/blockchain-parser
# btcpy: https://github.com/chainside/btcpy
#
# Author: Aggelos Stamatiou, April 2021
#
# This source code is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This software is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this source code. If not, see <http://www.gnu.org/licenses/>.
# --------------------------------------------------------------

import os
import datetime
import hashlib
from btcpy.setup import setup
from btcpy.structs.script import ScriptSig
from btcpy.structs.address import P2pkhAddress, P2wpkhAddress
from btcpy.structs.transaction import TransactionFactory, Transaction, TxIn, Sequence, TxOut, Locktime
from btcpy.structs.crypto import PublicKey
from btcpy.lib.codecs import Base58Codec
from btcpy.constants import Constants
from decimal import Decimal

# Given a RawTX, output format is generated using the btcpy library.
def create_record(resList, RawTX, Timestamp):
	tx = Transaction.unhexlify(RawTX)
	resList.append('tx,' + str(tx.txid) + ',' + Timestamp + ';')
	for txin in tx.ins:
		resList.append('txin,' + str(txin.txid) + ',' + str(tx.txid) + ',' + str(txin.txout) + ';')
	for txout in tx.outs:
		resList.append('txout,' + str(tx.txid) + ',' + str(txout.n) + ',' + str(txout.address()) + ',' + str(Decimal(txout.value) * Constants.get('from_unit')) + ';')

def reverse(input):
	L = len(input)
	if (L % 2) != 0:
		return None
	else:
		Res = ''
		L = L // 2
		for i in range(L):
			T = input[i*2] + input[i*2+1]
			Res = T + Res
			T = ''
		return (Res);

def merkle_root(lst):
	sha256d = lambda x: hashlib.sha256(hashlib.sha256(x).digest()).digest()
	hash_pair = lambda x, y: sha256d(x[::-1] + y[::-1])[::-1]
	if len(lst) == 1: return lst[0]
	if len(lst) % 2 == 1:
		lst.append(lst[-1])
	return merkle_root([hash_pair(x,y) for x, y in zip(*[iter(lst)]*2)])

def read_bytes(file,n,byte_order = 'L'):
	data = file.read(n)
	if byte_order == 'L':
		data = data[::-1]
	data = data.hex().upper()
	return data

def read_varint(file):
	b = file.read(1)
	bInt = int(b.hex(),16)
	c = 0
	data = ''
	if bInt < 253:
		c = 1
		data = b.hex().upper()
	if bInt == 253: c = 3
	if bInt == 254: c = 5
	if bInt == 255: c = 9
	for j in range(1,c):
		b = file.read(1)
		b = b.hex().upper()
		data = b + data
	return data

setup('mainnet')

dirA = '{path to Bitcoin blk files folder}'
dirB = 'parser_output/'

fList = os.listdir(dirA)
fList = [x for x in fList if (x.endswith('.dat') and x.startswith('blk'))]
fList.sort()

for i in fList:
	nameSrc = i
	nameRes = nameSrc.replace('.dat','.txt')
	resList = []
	a = 0
	t = dirA + nameSrc
	print ('Start ' + t + ' in ' + str(datetime.datetime.now()))
	f = open(t,'rb')
	tmpHex = ''
	fSize = os.path.getsize(t)
	while f.tell() != fSize:
		tmpHex = read_bytes(f,4)
		tmpHex = read_bytes(f,4)
		tmpPos3 = f.tell()
		tmpHex = read_bytes(f,80,'B')
		tmpHex = bytes.fromhex(tmpHex)
		tmpHex = hashlib.new('sha256', tmpHex).digest()
		tmpHex = hashlib.new('sha256', tmpHex).digest()
		tmpHex = tmpHex[::-1]		 
		tmpHex = tmpHex.hex().upper()
		f.seek(tmpPos3,0)
		tmpHex = read_bytes(f,4)
		tmpHex = read_bytes(f,32)
		tmpHex = read_bytes(f,32)
		MerkleRoot = tmpHex
		tmpHex = read_bytes(f,4)
		Timestamp = str(datetime.datetime.fromtimestamp(int(tmpHex, 16)).isoformat())
		tmpHex = read_bytes(f,4)
		tmpHex = read_bytes(f,4)
		tmpHex = read_varint(f)
		txCount = int(tmpHex,16)
		tmpHex = ''; RawTX = ''; tx_hashes = []
		for k in range(txCount):
			tmpHex = read_bytes(f,4)
			RawTX = reverse(tmpHex)
			tmpHex = ''
			Witness = False
			b = f.read(1)
			tmpB = b.hex().upper()
			bInt = int(b.hex(),16)
			if bInt == 0:
				tmpB = ''
				f.seek(1,1)
				c = 0
				c = f.read(1)
				bInt = int(c.hex(),16)
				tmpB = c.hex().upper()
				Witness = True
			c = 0
			if bInt < 253:
				c = 1
				tmpHex = hex(bInt)[2:].upper().zfill(2)
				tmpB = ''
			if bInt == 253: c = 3
			if bInt == 254: c = 5
			if bInt == 255: c = 9
			for j in range(1,c):
				b = f.read(1)
				b = b.hex().upper()
				tmpHex = b + tmpHex
			inCount = int(tmpHex,16)
			tmpHex = tmpHex + tmpB
			RawTX = RawTX + reverse(tmpHex)
			for m in range(inCount):
				tmpHex = read_bytes(f,32)
				RawTX = RawTX + reverse(tmpHex)
				tmpHex = read_bytes(f,4)				
				RawTX = RawTX + reverse(tmpHex)
				tmpHex = ''
				b = f.read(1)
				tmpB = b.hex().upper()
				bInt = int(b.hex(),16)
				c = 0
				if bInt < 253:
					c = 1
					tmpHex = b.hex().upper()
					tmpB = ''
				if bInt == 253: c = 3
				if bInt == 254: c = 5
				if bInt == 255: c = 9
				for j in range(1,c):
					b = f.read(1)
					b = b.hex().upper()
					tmpHex = b + tmpHex
				scriptLength = int(tmpHex,16)
				tmpHex = tmpHex + tmpB
				RawTX = RawTX + reverse(tmpHex)
				tmpHex = read_bytes(f,scriptLength,'B')
				RawTX = RawTX + tmpHex
				tmpHex = read_bytes(f,4,'B')
				RawTX = RawTX + tmpHex
				tmpHex = ''
			b = f.read(1)
			tmpB = b.hex().upper()
			bInt = int(b.hex(),16)
			c = 0
			if bInt < 253:
				c = 1
				tmpHex = b.hex().upper()
				tmpB = ''
			if bInt == 253: c = 3
			if bInt == 254: c = 5
			if bInt == 255: c = 9
			for j in range(1,c):
				b = f.read(1)
				b = b.hex().upper()
				tmpHex = b + tmpHex
			outputCount = int(tmpHex,16)
			tmpHex = tmpHex + tmpB
			RawTX = RawTX + reverse(tmpHex)
			for m in range(outputCount):
				tmpHex = read_bytes(f,8)
				Value = tmpHex
				RawTX = RawTX + reverse(tmpHex)
				tmpHex = ''
				b = f.read(1)
				tmpB = b.hex().upper()
				bInt = int(b.hex(),16)
				c = 0
				if bInt < 253:
					c = 1
					tmpHex = b.hex().upper()
					tmpB = ''
				if bInt == 253: c = 3
				if bInt == 254: c = 5
				if bInt == 255: c = 9
				for j in range(1,c):
					b = f.read(1)
					b = b.hex().upper()
					tmpHex = b + tmpHex
				scriptLength = int(tmpHex,16)
				tmpHex = tmpHex + tmpB
				RawTX = RawTX + reverse(tmpHex)
				tmpHex = read_bytes(f,scriptLength,'B')
				RawTX = RawTX + tmpHex
				tmpHex = ''
			if Witness == True:
				for m in range(inCount):
					tmpHex = read_varint(f)
					WitnessLength = int(tmpHex,16)
					for j in range(WitnessLength):
						tmpHex = read_varint(f)
						WitnessItemLength = int(tmpHex,16)
						tmpHex = read_bytes(f,WitnessItemLength)
						tmpHex = ''
			Witness = False
			tmpHex = read_bytes(f,4)
			RawTX = RawTX + reverse(tmpHex)
			tmpHex = RawTX
			tmpHex = bytes.fromhex(tmpHex)
			tmpHex = hashlib.new('sha256', tmpHex).digest()
			tmpHex = hashlib.new('sha256', tmpHex).digest()
			tmpHex = tmpHex[::-1]
			tmpHex = tmpHex.hex().upper()
			create_record(resList, RawTX, Timestamp)
			tx_hashes.append(tmpHex)
			tmpHex = ''; RawTX = ''
		a += 1
		tx_hashes = [bytes.fromhex(h) for h in tx_hashes]
		tmpHex = merkle_root(tx_hashes).hex().upper()
		if tmpHex != MerkleRoot:
			print ('Merkle roots does not match! >',MerkleRoot,tmpHex)
	f.close()
	f = open(dirB + nameRes,'w')
	for j in resList:
		f.write(j + '\n')
	f.close()
