import { pool } from "./dbConnection.js";

export const insertIntoPainelProcedimentos = async (
  i_ano_compt,
  i_mes_compt,
  i_desc_regiao_saude,
  i_ra_ses_desc,
  i_estab_cnes,
  i_sigla_estab_cnes,
  i_faixa_etaria,
  i_desc_sexo,
  i_ine,
  i_cat_prof,
  grupo_procedimento,
  i_cod_procedimento,
  i_desc_procedimento,
  i_qtd
) => {
  const client = await pool.connect();

  try {
    await client.query(
      `
        INSERT INTO painel_procedimentos (
          i_ano_compt, 
          i_mes_compt, 
          i_desc_regiao_saude, 
          i_ra_ses_desc, 
          i_estab_cnes, 
          i_sigla_estab_cnes,
          i_faixa_etaria, 
          i_desc_sexo, 
          i_ine, 
          i_cat_prof, 
          grupo_procedimento, 
          i_cod_procedimento, 
          i_desc_procedimento, 
          i_qtd
        ) 
        VALUES (
          $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
        )
        --ON CONFLICT (i_ano_compt, i_mes_compt, i_estab_cnes, i_cod_procedimento)
        --DO NOTHING;
        `,
      [
        i_ano_compt,
        i_mes_compt,
        i_desc_regiao_saude,
        i_ra_ses_desc,
        i_estab_cnes,
        i_sigla_estab_cnes,
        i_faixa_etaria,
        i_desc_sexo,
        i_ine,
        i_cat_prof,
        grupo_procedimento,
        i_cod_procedimento,
        i_desc_procedimento,
        i_qtd,
      ]
    );
  } catch (error) {
    console.error(`Error in insertIntoPainelProcedimentos: ${error}
  
    i_ano_compt: ${i_ano_compt},
    i_mes_compt: ${i_mes_compt},
    i_desc_regiao_saude: ${i_desc_regiao_saude},
    i_ra_ses_desc: ${i_ra_ses_desc},
    i_estab_cnes: ${i_estab_cnes},
    i_sigla_estab_cnes: ${i_sigla_estab_cnes},
    i_faixa_etaria: ${i_faixa_etaria},
    i_desc_sexo: ${i_desc_sexo},
    i_ine: ${i_ine},
    i_cat_prof: ${i_cat_prof},
    grupo_procedimento: ${grupo_procedimento},
    i_cod_procedimento: ${i_cod_procedimento},
    i_desc_procedimento: ${i_desc_procedimento},
    i_qtd: ${i_qtd}
        `);

    throw new Error(
      `Failed to execute insertIntoPainelProcedimentos: ${error}`
    );
  } finally {
    client.release();
  }
};
