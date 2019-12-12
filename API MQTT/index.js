var mqtt = require('mqtt');
var client = mqtt.connect('mqtt://192.168.102.45');

//MSSQL
const sql = require("mssql");
const config = {
    user: 'sa',
    password: 'password1234',
    server: 'localhost\\SQLEXPRESS',
    database: 'Calcetto-IOT',
    options: {
        encrypt: true
    }
};

client.on('connect', function () {
    client.subscribe('calcetto/#', async (err) => {});
});

client.on('message', async (topic, message) => {
    switch (topic) {
        case "calcetto/nick":
            if (message.toString() === "nick") {
                try {
                    let pool = await sql.connect(config);
                    let result = await pool.request()
                        .query `SELECT id, nickname  
                                        FROM dbo.partecipanti`;
                    sql.close();
                    client.publish('calcetto/nick', JSON.stringify(result.recordset));
                } catch (error) {
                    sql.close();
                    client.publish('calcetto/nick', error);
                }
            }
            console.log(message.toString());
            break;
        case "calcetto/teams":
                var data = JSON.parse(message.toString());
                try {
                    let pool = await sql.connect(config);
                    let result = await pool.request()
                        .input("nome", sql.VarChar, data.nome)
                        .input("atk", sql.Int, data.atk)
                        .input("def", sql.Int, data.def)
                        .query `INSERT INTO [dbo].[squadre]
                                    ([nome]
                                    ,[atk]
                                    ,[def])
                                VALUES
                                    (@nome
                                    ,@atk
                                    ,@def)`;
                    sql.close();
                    client.publish('calcetto/teams', "inserimento effettuato con successo, recordset: " + result.recordset);
                } catch (error) {
                    sql.close();
                    client.publish('calcetto/teams', error);
                }
            break;
        case "calcetto/matches":
                var data = JSON.parse(message.toString());
                try {
                    let pool = await sql.connect(config);
                    let squad1Res = await pool.request()
                        .input("s1", sql.VarChar, data.s1)
                        .query `SELECT id 
                            FROM dbo.squadre
                            WHERE nome = @s1`;

                    let squad2Res = await pool.request()
                        .input("s2", sql.VarChar, data.s2)
                        .query `SELECT id 
                            FROM dbo.squadre
                            WHERE nome = @s2`;

                    let result = await pool.request()
                        .input("s1", sql.Int, squad1Res.recordset[0].id)
                        .input("s2", sql.Int, squad2Res.recordset[0].id)
                        .input("goal1", sql.Int, data.g1)
                        .input("goal2", sql.Int, data.g2)
                        .input("rull1", sql.Int, data.r1)
                        .input("rull2", sql.Int, data.r2)
                        .input("miss1", sql.Int, data.m1)
                        .input("miss2", sql.Int, data.m2)
                        .query `
                                    INSERT INTO [dbo].[partite]
                                            ([s1]
                                            ,[s2]
                                            ,[goal1]
                                            ,[goal2]
                                            ,[rullate1]
                                            ,[rullate2]
                                            ,[miss1]
                                            ,[miss2])
                                        VALUES
                                            (@s1
                                            ,@s2
                                            ,@goal1
                                            ,@goal2
                                            ,@rull1
                                            ,@rull2
                                            ,@miss1
                                            ,@miss2)`;

                    sql.close();
                    client.publish('calcetto/matches', "registrazione partita effettuata con successo, recordset: " + result.recordset);
                } catch (error) {
                    sql.close();
                    client.publish('calcetto/matches', error);
                }
            break;

        default:
            break;
    }

});