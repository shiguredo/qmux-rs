//! ストリーム ID とストリーム状態の単体テスト。
//!
//! RFC 9000 Section 2.1 のストリーム ID のビット割り当て、可変長整数で表現できる
//! 範囲の境界、状態の終端判定といった、性質テストでは扱いにくい境界値を検証する。

use shiguredo_qmux::codec::role::Role;
use shiguredo_qmux::codec::varint;
use shiguredo_qmux::stream::{RecvState, SendState, StreamId, StreamType};

#[test]
fn stream_type_is_derived_from_low_bits() {
    // RFC 9000 Section 2.1: 下位 2 ビットが種別を表す
    assert_eq!(
        StreamId::new(0).expect("作成できる").stream_type(),
        StreamType::ClientBidi
    );
    assert_eq!(
        StreamId::new(1).expect("作成できる").stream_type(),
        StreamType::ServerBidi
    );
    assert_eq!(
        StreamId::new(2).expect("作成できる").stream_type(),
        StreamType::ClientUni
    );
    assert_eq!(
        StreamId::new(3).expect("作成できる").stream_type(),
        StreamType::ServerUni
    );

    assert!(StreamType::ClientBidi.is_bidirectional());
    assert!(StreamType::ServerBidi.is_bidirectional());
    assert!(!StreamType::ClientUni.is_bidirectional());
    assert!(!StreamType::ServerUni.is_bidirectional());
    assert!(StreamType::ClientBidi.is_client_initiated());
    assert!(!StreamType::ServerUni.is_client_initiated());
}

#[test]
fn stream_type_local_matches_role() {
    assert_eq!(
        StreamType::local(Role::Client, true),
        StreamType::ClientBidi,
        "クライアントの双方向ストリームは 0x00 であること"
    );
    assert_eq!(
        StreamType::local(Role::Client, false),
        StreamType::ClientUni,
        "クライアントの単方向ストリームは 0x02 であること"
    );
    assert_eq!(
        StreamType::local(Role::Server, true),
        StreamType::ServerBidi,
        "サーバーの双方向ストリームは 0x01 であること"
    );
    assert_eq!(
        StreamType::local(Role::Server, false),
        StreamType::ServerUni,
        "サーバーの単方向ストリームは 0x03 であること"
    );
}

#[test]
fn stream_id_sequence_roundtrip() {
    for sequence in [0u64, 1, 2, 1_000_000] {
        for stream_type in [
            StreamType::ClientBidi,
            StreamType::ServerBidi,
            StreamType::ClientUni,
            StreamType::ServerUni,
        ] {
            let id = StreamId::from_parts(stream_type, sequence).expect("作成できる");
            assert_eq!(id.stream_type(), stream_type, "種別が一致すること");
            assert_eq!(id.sequence(), sequence, "シーケンス番号が一致すること");
        }
    }
}

#[test]
fn stream_id_new_rejects_values_beyond_varint() {
    assert!(
        StreamId::new(varint::MAX_VALUE).is_some(),
        "可変長整数の最大値は作成できること"
    );
    assert!(
        StreamId::new(varint::MAX_VALUE + 1).is_none(),
        "可変長整数の最大値を超える値は作成できないこと"
    );
    assert!(
        StreamId::new(u64::MAX).is_none(),
        "u64 の最大値も拒否すること"
    );
}

#[test]
fn stream_id_from_parts_rejects_values_beyond_varint() {
    // 4 * sequence + 種別が 2^62-1 を超えない最大のシーケンス番号
    let max_sequence = varint::MAX_VALUE / 4;
    assert!(
        StreamId::from_parts(StreamType::ClientBidi, max_sequence).is_some(),
        "上限内のシーケンス番号は作成できること"
    );
    assert!(
        StreamId::from_parts(StreamType::ClientBidi, max_sequence + 1).is_none(),
        "上限を超えるシーケンス番号は作成できないこと"
    );
}

#[test]
fn stream_id_from_parts_handles_huge_sequence_without_overflow() {
    // 乗算がオーバーフローする値でも panic せず None を返すこと。
    // (debug ビルドでは通常の乗算が panic するため、checked_* の回帰確認になる)
    assert!(
        StreamId::from_parts(StreamType::ServerUni, 1 << 62).is_none(),
        "オーバーフローする値は None になること"
    );
    assert!(
        StreamId::from_parts(StreamType::ServerUni, u64::MAX).is_none(),
        "u64 の最大値でも None になること"
    );
}

#[test]
fn stream_id_is_local_matches_role() {
    let client_bidi = StreamId::from_parts(StreamType::ClientBidi, 0).expect("作成できる");
    let server_bidi = StreamId::from_parts(StreamType::ServerBidi, 0).expect("作成できる");
    let client_uni = StreamId::from_parts(StreamType::ClientUni, 0).expect("作成できる");
    let server_uni = StreamId::from_parts(StreamType::ServerUni, 0).expect("作成できる");

    assert!(
        client_bidi.is_local(Role::Client),
        "クライアントから見てローカル"
    );
    assert!(
        !client_bidi.is_local(Role::Server),
        "サーバーから見てピア起動"
    );
    assert!(
        server_bidi.is_local(Role::Server),
        "サーバーから見てローカル"
    );
    assert!(
        client_uni.is_local(Role::Client),
        "クライアントから見てローカル"
    );
    assert!(
        server_uni.is_local(Role::Server),
        "サーバーから見てローカル"
    );
}

#[test]
fn stream_states_are_terminal() {
    assert!(!SendState::Ready.is_terminal(), "Ready は終端ではないこと");
    assert!(
        SendState::DataRecvd.is_terminal(),
        "DataRecvd は終端であること"
    );
    assert!(
        SendState::ResetRecvd.is_terminal(),
        "ResetRecvd は終端であること"
    );

    assert!(!RecvState::Recv.is_terminal(), "Recv は終端ではないこと");
    assert!(
        !RecvState::SizeKnown.is_terminal(),
        "SizeKnown は終端ではないこと"
    );
    assert!(
        RecvState::DataRead.is_terminal(),
        "DataRead は終端であること"
    );
    assert!(
        !RecvState::ResetRecvd.is_terminal(),
        "ResetRecvd はアプリケーションが観測するまで終端ではないこと"
    );
    assert!(
        RecvState::ResetRead.is_terminal(),
        "ResetRead は終端であること"
    );
}
