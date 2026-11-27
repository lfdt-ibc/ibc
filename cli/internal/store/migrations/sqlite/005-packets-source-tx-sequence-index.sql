-- SPDX-License-Identifier: Apache-2.0

-- +migrate Up
-- ListPacketsBySourceTx orders a tx's packets by sequence. With the sequence in
-- this index, the index serves both the filter and the order. Without it,
-- SQLite with no statistics picks index_packet to avoid the sort and reads
-- every packet of the chain.
create index if not exists index_packets_source_tx_sequence
    on packets (source_chain_id, source_tx_hash, packet_sequence_number);
drop index if exists index_packets_source_tx;

-- +migrate Down
create index if not exists index_packets_source_tx
    on packets (source_chain_id, source_tx_hash);
drop index if exists index_packets_source_tx_sequence;
