use chrono::NaiveDate;
use postgres::{Connection, TlsMode};
use std::collections::HashMap;

struct UsrMessage {
    id: i32,
    message: String,
    slaktedato: NaiveDate,
    efta: i32,
    skrottnr: i64,
    posteringsside: String,
    duplicate: bool,
}

fn main() {
    // Hashmap for detection of duplicate messages by (slaktedato, efta, skrottnr)
    let mut duplicatemsg: HashMap<(NaiveDate, i32, i64), i32> = HashMap::new();

    let conn =
        Connection::connect("postgres://postgres:rpg@localhost:5432", TlsMode::None).unwrap();

    // conn.execute("DROP TABLE usrmessage", &[]).unwrap();

    conn.execute(
        "CREATE TABLE IF NOT EXISTS usrmessage (
                    id              SERIAL PRIMARY KEY,
                    message         VARCHAR NOT NULL,
                    slaktedato      DATE NOT NULL,
                    efta            INTEGER NOT NULL,
                    skrottnr        BIGINT NOT NULL,
                    posteringsside  VARCHAR NOT NULL,
                    duplicate       BOOL DEFAULT FALSE
                  )",
        &[],
    )
    .unwrap();

    for row in &conn
        .query(
            "SELECT id, message, slaktedato, efta, skrottnr, posteringsside, duplicate FROM usrmessage",
            &[],
        )
        .unwrap()
    {
        let usrmessage = UsrMessage {
            id: row.get(0),
            message: row.get(1),
            slaktedato: row.get(2),
            efta: row.get(3),
            skrottnr: row.get(4),
            posteringsside: row.get(5),
            duplicate: row.get(6),
        };
        println!(
            "id: {0}, message: {1}, slaktedato: {2}, efta: {3}, skrottnr: {4}, posteringsside {5}, duplicate: {6}",
            usrmessage.id,
            usrmessage.message,
            usrmessage.slaktedato,
            usrmessage.efta,
            usrmessage.skrottnr,
            usrmessage.posteringsside,
            usrmessage.duplicate,
        );
        // hash
        if !duplicatemsg.contains_key(&( // convert to counting
            usrmessage.slaktedato,
            usrmessage.efta,
            usrmessage.skrottnr,
        )) {
            duplicatemsg.insert(
                (usrmessage.slaktedato, usrmessage.efta, usrmessage.skrottnr),
                1,
            );
        } else {
            let updates = conn
                .execute(
                    "UPDATE usrmessage SET duplicate = true WHERE id = $1 AND duplicate = false",
                    &[&usrmessage.id],
                )
                .unwrap();
            println!("{} rows were updated", updates);
        }
        if usrmessage.id == 20 {
            conn.execute("DROP TABLE usrmessage", &[]).unwrap();
        };
    }
    for (key, value) in &duplicatemsg {
        println!("{} - {} - {} count: {}", key.0, key.1, key.2, value);
    }
}
