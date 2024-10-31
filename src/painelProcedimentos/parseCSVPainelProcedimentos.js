import fs from "fs";
import { parse } from "csv-parse";
import path from "path";
import { insertIntoPainelProcedimentos } from "../db/dbPainelProcedimentos.js";
import dotenv from "dotenv";

dotenv.config();

export const parseCSVPainelProcedimentos = () => {
  const pathCSV = path.resolve(process.env.DOC_PAINEL_PROCEDIMENTOS);

  let count = 0;

  console.log("start parse", new Date().toLocaleTimeString());

  const stream = fs
    .createReadStream(pathCSV)
    .pipe(parse({ delimiter: ",", from_line: 1, columns: true }));

  stream.on("data", async function (row) {
    stream.pause();

    count = count + 1;

    const newRow = {
      i_ano_compt: Number(row.i_ano_compt), // int
      i_mes_compt: Number(row.i_mes_compt), // int
      i_desc_regiao_saude: row.i_desc_regiao_saude, // varchar
      i_ra_ses_desc: row.i_ra_ses_desc, // varchar
      i_estab_cnes: isNaN(Number(row.i_estab_cnes))
        ? 0
        : Number(row.i_estab_cnes), // int
      i_sigla_estab_cnes: row.i_sigla_estab_cnes, // varchar
      i_faixa_etaria: row.i_faixa_etaria, // varchar
      i_desc_sexo: row.i_desc_sexo, // varchar
      i_ine: row.i_ine, // varchar
      i_cat_prof: row.i_cat_prof, // varchar
      grupo_procedimento: row.grupo_procedimento, // varchar
      i_cod_procedimento: isNaN(Number(row.i_cod_procedimento))
        ? 0
        : Number(row.i_cod_procedimento), // int
      i_desc_procedimento: row.i_desc_procedimento, // varchar
      i_qtd: isNaN(Number(row.i_qtd)) ? 0 : Number(row.i_qtd), // int
    };

    try {
      await insertIntoPainelProcedimentos(
        newRow.i_ano_compt,
        newRow.i_mes_compt,
        newRow.i_desc_regiao_saude,
        newRow.i_ra_ses_desc,
        newRow.i_estab_cnes,
        newRow.i_sigla_estab_cnes,
        newRow.i_faixa_etaria,
        newRow.i_desc_sexo,
        newRow.i_ine,
        newRow.i_cat_prof,
        newRow.grupo_procedimento,
        newRow.i_cod_procedimento,
        newRow.i_desc_procedimento,
        newRow.i_qtd
      );

      // console.log(`Inserção nª ${count} bem sucedida!`);
    } catch (error) {
      console.error(`Erro na inserção: ${error}`);
    }

    stream.resume();
  });

  stream.on("end", function () {
    console.log("finished parse", new Date().toLocaleTimeString());
    console.log(count);
  });

  stream.on("error", function (error) {
    console.log(error.message, new Date().toLocaleTimeString());
  });
};
