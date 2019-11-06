use chrono::NaiveDate;
use postgres::{Connection, TlsMode};
use std::collections::HashMap;

struct UsrMessage {
    id: i32,
    message: String,
    slaktedato: NaiveDate,
    efta: i32,
    skrottnr: i64,
    duplicate: bool,
}

fn main() {
    // Hashmap for detection of duplicate messages by (slaktedato, efta, skrottnr)
    let mut duplicatemsg: HashMap<(NaiveDate, i32, i64), bool> = HashMap::new();

    let conn =
        Connection::connect("postgres://postgres:rpg@localhost:5432", TlsMode::None).unwrap();

    conn.execute(
        "CREATE TABLE IF NOT EXISTS usrmessage (
                    id              SERIAL PRIMARY KEY,
                    message         VARCHAR NOT NULL,
                    slaktedato      DATE NOT NULL,
                    efta            INTEGER NOT NULL,
                    skrottnr        BIGINT NOT NULL,
                    duplicate       BOOL DEFAULT FALSE
                  )",
        &[],
    )
    .unwrap();

    let message = UsrMessage {
        id: 0,
        message: "abc".to_string(),
        slaktedato: NaiveDate::from_ymd(2015, 3, 14),
        efta: 140,
        skrottnr: 12345678,
        duplicate: false,
    };

    conn.execute("INSERT INTO usrmessage (message, slaktedato, efta, skrottnr, duplicate) VALUES ($1, $2, $3, $4, $5)",
                 &[&message.message, &message.slaktedato, &message.efta, &message.skrottnr , &message.duplicate]).unwrap();

    let message = UsrMessage {
        id: 0,
        message: "abc".to_string(),
        slaktedato: NaiveDate::from_ymd(2016, 3, 14),
        efta: 140,
        skrottnr: 12345678,
        duplicate: false,
    };

    conn.execute("INSERT INTO usrmessage (message, slaktedato, efta, skrottnr, duplicate) VALUES ($1, $2, $3, $4, $5)",
                 &[&message.message, &message.slaktedato, &message.efta, &message.skrottnr , &message.duplicate]).unwrap();

    for row in &conn
        .query(
            "SELECT id, message, slaktedato, efta, skrottnr, duplicate FROM usrmessage",
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
            duplicate: row.get(5),
        };
        println!(
            "id: {0}, message: {1}, slaktedato: {2}, efta: {3}, skrottnr: {4}, duplicate: {5}",
            usrmessage.id,
            usrmessage.message,
            usrmessage.slaktedato,
            usrmessage.efta,
            usrmessage.skrottnr,
            usrmessage.duplicate
        );
        // hash
        if !duplicatemsg.contains_key(&(
            usrmessage.slaktedato,
            usrmessage.efta,
            usrmessage.skrottnr,
        )) {
            duplicatemsg.insert(
                (usrmessage.slaktedato, usrmessage.efta, usrmessage.skrottnr),
                true,
            );
        } else {
            let updates = conn
                .execute(
                    "UPDATE usrmessage SET duplicate = true WHERE id = $1",
                    &[&usrmessage.id],
                )
                .unwrap();
            println!("{} rows were updated", updates);
        }
        if usrmessage.id == 20 {
            conn.execute("DROP TABLE usrmessage", &[]).unwrap();
        };
    }
    for (key, _) in &duplicatemsg {
        println!("{} - {} - {}", key.0, key.1, key.2);
    }
}
