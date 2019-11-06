use postgres::{Connection, TlsMode};
use chrono::{NaiveDate};

struct UsrMessage {
    id: i32,
    slaktedato: NaiveDate,
    efta: i32,
    message: String,
    skrottnr: i64,
    duplicate: bool,
}

fn main() {
    let conn = Connection::connect("postgres://postgres:rpg@localhost:5432", TlsMode::None).unwrap();

     conn.execute("CREATE TABLE IF NOT EXISTS usrmessage (
                    id              SERIAL PRIMARY KEY,
                    slaktedato      DATE NOT NULL,
                    efta            INTEGER NOT NULL,
                    message         VARCHAR NOT NULL,
                    skrottnr        BIGINT NOT NULL,
                    duplicate       BOOL DEFAULT FALSE
                  )", &[]).unwrap();

 
    let message = UsrMessage {
        id: 0,
        slaktedato: NaiveDate::from_ymd(2015, 3, 14),
        efta: 140,
        message: "abc".to_string(),
        skrottnr: 12345678,
        duplicate: false,
    };

    conn.execute("INSERT INTO usrmessage (slaktedato, efta, message, skrottnr, duplicate) VALUES ($1, $2, $3, $4, $5)",
                 &[&message.slaktedato, &message.efta, &message.message , &message.skrottnr , &message.duplicate]).unwrap();

    for row in &conn.query("SELECT id, slaktedato, efta, message, skrottnr, duplicate FROM usrmessage", &[]).unwrap() {
        let usrmessage = UsrMessage {
            id: row.get(0),
            slaktedato: row.get(1),
            efta: row.get(2),
            message: row.get(3),
            skrottnr: row.get(4),
            duplicate: row.get(5),
        };
        println!("id: {0}, slaktedato: {1}, efta: {2}, message: {3}, skrottnr: {4}, duplicate: {5}", usrmessage.id, usrmessage.slaktedato, usrmessage.efta, usrmessage.message, usrmessage.skrottnr, usrmessage.duplicate);
        if usrmessage.id==10 {
            conn.execute("DROP TABLE usrmessage", &[]).unwrap();
        };   
    }
}