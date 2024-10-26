import fs from "fs";
import { parse } from "csv-parse";
import path from "path";

export const parseCSVPainelProcedimentos = () => {
  const pathCSV = path.resolve(
    "/home/leal/Documentos/Dev/Dev-jobs/freelas/infosaude-gabriel/csv/painel_procedimentos/dados_aps_procedimentos-25102024-anos_2021-meses_01.csv"
  );

  let count = 0;

  console.log("start parse", new Date().toLocaleTimeString());
  fs.createReadStream(pathCSV)
    .pipe(parse({ delimiter: ",", from_line: 1, columns: true }))
    .on("data", function (row) {
      const newRow = {
        i_ano_compt: Number(row.i_ano_compt), // int
        i_mes_compt: Number(row.i_mes_compt), // int
        i_desc_regiao_saude: row.i_desc_regiao_saude, // varchar
        i_ra_ses_desc: row.i_ra_ses_desc, // varchar
        i_estab_cnes: Number(row.i_estab_cnes), // int
        i_sigla_estab_cnes: row.i_sigla_estab_cnes, // varchar
        i_faixa_etaria: row.i_faixa_etaria, // varchar
        i_desc_sexo: row.i_desc_sexo, // varchar
        i_ine: row.i_ine, // varchar
        i_cat_prof: row.i_cat_prof, // varchar
        grupo_procedimento: row.grupo_procedimento, // varchar
        i_cod_procedimento:
          row.i_cod_procedimento !== "Não possui"
            ? Number(row.i_cod_procedimento)
            : 0, // int
        i_desc_procedimento: row.i_desc_procedimento, // varchar
        i_qtd: Number(row.i_qtd), // int
      };

      if (newRow.i_cod_procedimento === 0) {
        console.log(newRow);

        count = count + 1;
      }

      // return newRow;
    })
    .on("end", function () {
      console.log("finished parse", new Date().toLocaleTimeString());
      console.log(count);
    })
    .on("error", function (error) {
      console.log(error.message, new Date().toLocaleTimeString());
    });
};
