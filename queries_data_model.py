from settings import config
from google.cloud import bigquery
from os_environ import osEnviron

PROJECT_ID = config.PROJECT_ID
DATA_SET_PRC = config.DATA_SET_PRC
DATA_SET_DATA_MODEL = config.DATA_SET_DATA_MODEL

osEnviron.set_os_environ(config.KEY_PATH)
client = bigquery.Client(project = PROJECT_ID)

class QueriesDataModel:
    
    def QueryBdPrime(self, client):
        
        SQL = f"""
            insert into `{PROJECT_ID}.{DATA_SET_DATA_MODEL}.Survey`
           (
            SurveyID,
            Domain,
            CompanyID,
            CompanyDescription,
            BranchOfficeID,
            BranchOfficeDescription,
            SalesOfficeDescription,
            SurveyDate,
            SurveyEndDate,
            CustomerID,
            CustomerIDDBS,
            CustomerName,
            ModelDescription,
            FamilyDescription,
            SerialNumber,
            AssignmentReport,
            SurveyContactName,
            SurveyContactJobPosition,
            SurveyContactPhoneNumber,
            SurveyContactEmail,
            SatisfactionEvaluation,
            BuybackEvaluation,
            RecommendationEvaluation,
            AttentionEvaluation,
            ConfigurationEvaluation,
            PerformanceEvaluation,
            PhysicalDeliveryEvaluation,
            TechnicalDeliveryEvaluation,
            SatisfactionComment,
            BuybackComment,
            RecommendationComment,
            AttentionComment,
            ConfigurationComment,
            PerformanceComment,
            PhysicalDeliveryComment,
            TechnicalDeliveryComment,
            HigherRatingComment,
            AdditionalComment,
            RegionDescription,
            ResponsibleManagement,
            SalesRepGroupDescription,
            CustomerSegmentDescription,
            AditionalCustomerSegmentDescription,
            CustomerMarketDescription,
            RRVV,
            PSSR,
            SurveyCounter,
            LoyaltyIndex,
            NetLoyaltyScoreDescription,
            NetLoyaltyScore,
            ExecutedBy,
            CustomerType,
            ParentCustomerName,
            AttendedBy,
            BrandDescription,
            LineDescription,
            SurveyOrigin,
            Period,
            UniqueID
            )
            SELECT 
            CASE
                WHEN REPORTE_DE_ASIGNACION = 'None' THEN null
                else REPORTE_DE_ASIGNACION
            END,
            'PRIME',
            CASE
                WHEN ORGANIZACION = 'Ferreyros' THEN '2000'
                ELSE NULL
            END,
            ORGANIZACION,
            NRO_SUCURSAL,
            SUCURSAL,
            OFICINA,
            MES_ENCUESTA,
            FECHA_DE_FINALIZACION_DE_LA_ENCUESTA,
            CODIGO_DE_CLIENTE,
            CODIGO_DE_CLIENTE,
            NOMBRE_DEL_CLIENTE,
            CASE
                WHEN IND_MODELO = 'None' THEN null
                else IND_MODELO
            END,
            CASE
                WHEN FAMILIADEPRODUCTOS = 'None' THEN null
                else FAMILIADEPRODUCTOS
            END,
            CASE
                WHEN SERIE = 'None' THEN null
                else SERIE
            END,
            CASE
                WHEN REPORTE_DE_ASIGNACION = 'None' THEN null
                else REPORTE_DE_ASIGNACION
            END,
            CASE
                WHEN CONTACTO_ENCUESTA = 'None' THEN null
                else CONTACTO_ENCUESTA
            END,
            CASE
                WHEN CARGO = 'None' THEN null
                else CARGO
            END,
            CASE
                WHEN TELEFONO = 'None' THEN null
                else TELEFONO
            END,
            CASE
                WHEN CORREO_ELECTRONICO = 'None' THEN null
                else CORREO_ELECTRONICO
            END,
            SATISFACCION,
            RECOMPRA,
            RECOMENDACION,
            ATENCION,
            CONFIGURACION,
            DESEMPENIO,
            ENTREGA_FISICA,
            ENTREGA_TECNICA,
            CASE
                WHEN COMENTARIO_SATISFACCION = 'None' THEN null
                else COMENTARIO_SATISFACCION
            END,
            CASE
                WHEN COMENTARIO_RECOMPRA = 'None' THEN null
                else COMENTARIO_RECOMPRA
            END,
            CASE
                WHEN COMENTARIO_RECOMENDACION = 'None' THEN null
                else COMENTARIO_RECOMENDACION
            END,
            CASE
                WHEN COMENTARIO_ATENCION = 'None' THEN null
                else COMENTARIO_ATENCION
            END,
            CASE
                WHEN COMENTARIO_CONFIGURACION = 'None' THEN null
                else COMENTARIO_CONFIGURACION
            END,
            CASE
                WHEN COMENTARIO_DESEMPENIO = 'None' THEN null
                else COMENTARIO_DESEMPENIO
            END,
            CASE
                WHEN COMENTARIO_ENTREGA_FISICA = 'None' THEN null
                else COMENTARIO_ENTREGA_FISICA
            END,
            CASE
                WHEN COMENTARIO_ENTREGA_TECNICA = 'None' THEN null
                else COMENTARIO_ENTREGA_TECNICA
            END,
            CASE
                WHEN MAYOR_VALORACION = 'None' THEN null
                else MAYOR_VALORACION
            END,
            CASE
                WHEN COMENTARIO_ADICIONAL = 'None' THEN null
                else COMENTARIO_ADICIONAL
            END,
            CASE
                WHEN REGION = 'None' THEN null
                else REGION
            END,
            CASE
                WHEN GERENCIA_RESPONSABLE = 'None' THEN null
                else GERENCIA_RESPONSABLE
            END,
            UNIDAD_DE_NEGOCIO,
            SEGMENTO,
            CASE
                WHEN SEGMENTO_2 = 'None' THEN null
                else SEGMENTO_2
            END,
            MERCADO,
            CASE
                WHEN RRVV = 'None' THEN null
                else RRVV
            END,
            CASE
                WHEN PSSR = 'None' THEN null
                else PSSR
            END,
            NRO_ENCUESTAS,
            CAST(INDICE_DE_LEALTAD AS NUMERIC),
            NLS2,
            CAST(NLS AS NUMERIC),
            EJECUTADA_POR,
            TIPO_DE_RAZON_SOCIAL,
            RAZON_SOCIAL_PARENT,
            ATENDIDO_POR,
            MARCA,
            LINEA,
            ORIGEN_ENCUESTA,
            PERIODO_CARGA,
            identificador_unico
            FROM {PROJECT_ID}.{DATA_SET_PRC}.BD_PRIME_SQL
        """
        
        query_job = client.query(SQL)
        query_job.result()
        
        print(f'Tabla {PROJECT_ID}.{DATA_SET_DATA_MODEL}.Survey actualizada.')
        

    def QueryBdRental(self, client):
        
        SQL = f"""
        insert into {PROJECT_ID}.{DATA_SET_DATA_MODEL}.Survey
        (
        SurveyID,
        Domain,
        CompanyID,
        CompanyDescription,
        BranchOfficeID,
        BranchOfficeDescription,
        SalesOfficeDescription,
        SurveyDate,
        SurveyEndDate,
        CustomerID,
        CustomerIDDBS,
        CustomerName,
        CustomerPhomeNumber,
        ComponentCode,
        SerialNumber,
        SurveyContactName,
        SurveyContactPhoneNumber,
        SurveyContactEmail,
        SurveyPerson,
        SurveyPersonNumber,
        SatisfactionEvaluation,
        BuybackEvaluation,
        RecommendationEvaluation,
        EaseDoingBusinessEvaluation,
        AvailabilityEvaluation,
        AnswerEvaluation,
        CommunicationEvaluation,
        PreparationEvaluation,
        DurationEvaluation,
        QualityEvaluation,
        InvoicePunctualityEvaluation,
        InvoiceAccuracyEvaluation,
        SatisfactionComment,
        BuybackComment,
        RecommendationComment,
        EaseDoingBusinessComment,
        AvailabilityComment,
        AnswerComment,
        CommunicationComment,
        PreparationComment,
        DurationComment,
        QualityComment,
        InvoicePunctualityComment,
        InvoiceAccuracyComment,
        HigherRatingComment,
        AdditionalComment,
        AudioFile,
        RegionDescription,
        ResponsibleManagement,
        SalesRepGroupDescription,
        CustomerSegmentDescription,
        AditionalCustomerSegmentDescription,
        CustomerMarketDescription,
        RRVV,
        PSSR,
        SurveyCounter,
        LoyaltyIndex,
        NetLoyaltyScoreDescription,
        NetLoyaltyScore,
        CustomerType,
        ParentCustomerName,
        AttendedBy,
        BrandDescription,
        LineDescription,
        SurveyOrigin,
        Period

        )
        SELECT 
        CAST(EXTRACT(MONTH FROM DATE (MES_ENCUESTA)) AS STRING) || '-' || NUMERO_CLIENTE || '-' || Serie,
        'RENTAL'	,
        CASE
            WHEN ORGANIZACION = 'Ferreyros' THEN '2000'
            ELSE NULL
        END,
        CASE
            WHEN ORGANIZACION = 'None' THEN null
            else ORGANIZACION
        END,
        CASE
            WHEN NRO_SUCURSAL = 'None' THEN null
            else NRO_SUCURSAL
        END,
        CASE
            WHEN SUCURSAL = 'None' THEN null
            else SUCURSAL
        END,
        CASE
            WHEN OFICINA = 'None' THEN null
            else OFICINA
        END,
        MES_ENCUESTA,
        FECHA_FINALIZACION_LA_ENCUESTA,
        CASE
            WHEN NUMERO_CLIENTE = 'None' THEN null
            else NUMERO_CLIENTE
        END,
        CASE
            WHEN NUMERO_CLIENTE = 'None' THEN null
            else NUMERO_CLIENTE
        END,
        CASE
            WHEN NOMBRE_DEL_CLIENTE = 'None' THEN null
            else NOMBRE_DEL_CLIENTE
        END,
        CASE
            WHEN NUMERO_DEL_CLIENTE = 'None' THEN null
            else NUMERO_DEL_CLIENTE
        END,
        CASE
            WHEN CODIGO_COMPONENTE = 'None' THEN null
            else CODIGO_COMPONENTE
        END,
        CASE
            WHEN SERIE = 'None' THEN null
            else SERIE
        END,
        CASE
            WHEN CONTACTO_DE_CLIENTE = 'None' THEN null
            else CONTACTO_DE_CLIENTE
        END,
        CASE
            WHEN TELEFONO_CONTACTO = 'None' THEN null
            else TELEFONO_CONTACTO
        END,
        CASE
            WHEN CORREO_CONTACTO = 'None' THEN null
            else CORREO_CONTACTO
        END,

        PERSONA_QUE_CONTESTO_LA_ENCUESTA,
        TELEFONO,
        SATISFACCION_GENERAL,
        RECOMPRA,
        RECOMENDACION,
        FACILIDAD_PARA_EL_TRAMITE,
        DISPONIBILIDAD,
        RESPUESTA,
        COMUNICACION,
        PREPARACION,
        DURACION,
        CALIDAD,
        PUNTUALIDAD_LA_FACTURA,
        PRECISION_LA_FACTURA,
        CASE
            WHEN SATISFACCION_GENERAL_COMS = 'None' THEN null
            else SATISFACCION_GENERAL_COMS
        END,
        CASE
            WHEN RECOMPRA_COMS = 'None' THEN null
            else RECOMPRA_COMS
        END,
        CASE
            WHEN RECOMENDACION_COMS = 'None' THEN null
            else RECOMENDACION_COMS
        END,
        CASE
            WHEN FACILIDAD_PARA_EL_TRAMITE_COMS = 'None' THEN null
            else FACILIDAD_PARA_EL_TRAMITE_COMS
        END,
        CASE
            WHEN DISPONIBILIDAD_Y_FIABILIDAD_COMS = 'None' THEN null
            else DISPONIBILIDAD_Y_FIABILIDAD_COMS
        END,
        CASE
            WHEN RESPUESTA_COMS = 'None' THEN null
            else RESPUESTA_COMS
        END,
        COMUNICACION_COMS,
        PREPARACION_COMS,
        DURACION_COMS,
        CALIDAD_COMS,
        PUNTUALIDAD_FACTURA_COMS,
        PRECISION_FACTURA_COMS,
        MAYOR_VALORACION_COMS,
        COMENTARIOS_ADICIONALES,
        ARCHIVO_AUDIO,
        REGION,
        GERENCIA_RESPONSABLE,
        UNIDAD_NEGOCIO,
        SEGMENTO,
        CASE
            WHEN SEGMENTO_2 = 'nan' THEN null
            else SEGMENTO_2
        END,
        MERCADO,
        RRVV,
        PSSR,
        NRO_ENCUESTAS,
        CAST(INDICE_LEALTAD AS NUMERIC),
        NLS2,
        CAST(NLS AS NUMERIC),
        TIPO_RAZON_SOCIAL,
        RAZON_SOCIAL_PARENT,
        ATENDIDO_POR,
        MARCA,
        LINEA,
        ORIGEN_ENCUESTA,
        PERIODO_CARGA,
        FROM {PROJECT_ID}.{DATA_SET_PRC}.BD_RENTAL_SQL
        """
        
        query_job = client.query(SQL)
        query_job.result()
        
        print(f'Tabla {PROJECT_ID}.{DATA_SET_DATA_MODEL}.Survey actualizada.')
        
        
    def QueryBdRepuestos(self, client):
        
        SQL = f"""
        insert into {PROJECT_ID}.{DATA_SET_DATA_MODEL}.Survey
        (
        SurveyID,
        Domain,
        CompanyID,
        CompanyDescription,
        SurveyType,
        BranchOfficeID,
        BranchOfficeDescription,
        SalesOfficeDescription,
        SurveyDate,
        SurveyEndDate,
        CustomerID,
        CustomerIDDBS,
        CustomerName,
        InvoiceAmount,
        ReferenceDocument,
        ComponentCode,
        SurveyContactName,
        SurveyContactPhoneNumber,
        SurveyContactEmail,
        SurveyPerson,
        SurveyPersonNumber,
        SatisfactionEvaluation,
        BuybackEvaluation,
        RecommendationEvaluation,
        EaseDoingBusinessEvaluation,
        AvailabilityEvaluation,
        AnswerEvaluation,
        CommunicationEvaluation,
        PreparationEvaluation,
        DurationEvaluation,
        QualityEvaluation,
        InvoicePunctualityEvaluation,
        InvoiceAccuracyEvaluation,
        SatisfactionComment,
        BuybackComment,
        RecommendationComment,
        EaseDoingBusinessComment,
        AvailabilityComment,
        AnswerComment,
        CommunicationComment,
        PreparationComment,
        DurationComment,
        QualityComment,
        InvoicePunctualityComment,
        InvoiceAccuracyComment,
        HigherRatingComment,
        AdditionalComment,
        RegionDescription,
        ResponsibleManagement,
        SalesRepGroupDescription,
        CustomerSegmentDescription,
        AditionalCustomerSegmentDescription,
        CustomerMarketDescription,
        RRVV,
        PSSR,
        SurveyCounter,
        LoyaltyIndex,
        NetLoyaltyScoreDescription,
        NetLoyaltyScore,
        CustomerType,
        ParentCustomerName,
        AttendedBy,
        BrandDescription,
        LineDescription,
        SurveyOrigin,
        Period
        )
        SELECT 
        DOCUMENTO_REFERENCIA,
        'PARTS',
        CASE
            WHEN ORGANIZACION = 'Ferreyros' THEN '2000'
            ELSE NULL
        END,
        ORGANIZACION,
        TIPO_ENCUESTA,
        N_SUCURSAL,
        SUCURSAL,
        OFICINA,
        MES_ENCUESTA,
        FECHA_FINALIZACION_LA_ENCUESTA,
        NUMERO_CLIENTE,
        NUMERO_CLIENTE,
        NOMBRE_CLIENTE,
        MONTO_FACTURADO,
        DOCUMENTO_REFERENCIA,
        CODIGO_COMPONENTE,
        CONTACTO_CLIENTE,
        TELEFONO_CONTACTO,
        CORREO_CONTACTO,
        PERSONA_QUE_RESPONDIO_LA_ENCUESTA,
        TELEFONO,
        SATISFACCION_GENERAL,
        RECOMPRA,
        RECOMENDACION,
        FACILIDAD_PARA_EL_TRAMITE,
        DISPONIBILIDAD_CONOCIMIENTO_VENDEDOR,
        RESPUESTA,
        COMUNICACION,
        PREPARACION_STOCK,
        DURACION,
        CALIDAD,
        PUNTUALIDAD_FACTURA,
        PRECISION_FACTURA,
        CASE
            WHEN SATISFACCION_GENERAL_COMENTARIO = 'None' THEN null
            else SATISFACCION_GENERAL_COMENTARIO 
        END,
        CASE
            WHEN RECOMPRA_COMENTARIO = 'None' THEN null
            else RECOMPRA_COMENTARIO
        END,
        CASE
            WHEN RECOMENDACION_COMENTARIO = 'None' THEN null
            else RECOMENDACION_COMENTARIO
        END,
        CASE
            WHEN FACILIDAD_PARA_EL_TRAMITE_COMENTARIO = 'None' THEN null
            else FACILIDAD_PARA_EL_TRAMITE_COMENTARIO
        END,
        CASE
            WHEN DISPONIBILIDAD_Y_FIABILIDAD_COMENTARIO = 'None' THEN null
            else DISPONIBILIDAD_Y_FIABILIDAD_COMENTARIO
        END,
        CASE
            WHEN RESPUESTA_COMENTARIO = 'None' THEN null
            else RESPUESTA_COMENTARIO
        END,
        COMUNICACION_COMENTARIO,
        PREPARACION_COMS,
        DURACION_COMENTARIO,
        CALIDAD_COMENTARIO,
        PUNTUALIDAD_FACTURA_COMENTARIO,
        PRECISION_FACTURA_COMENTARIO,
        MAYOR_VALORACION_COMENTARIO,
        COMENTARIOS_ADICIONALES,
        REGION,
        GERENCIA_RESPONSABLE,
        UNIDAD_NEGOCIOS,
        SEGMENTO,
        SEGMENTO_2,
        MERCADO,
        RRVV,
        PSSR,
        NRO_ENCUESTAS,
        CAST(INDICE_LEALTAD AS NUMERIC),
        NLS2,
        CAST(NLS AS NUMERIC),
        TIPO_RAZON_SOCIAL,
        RAZON_SOCIAL_PARENT,
        ATENDIDO_POR,
        MARCA,
        LINEA,
        ORIGEN_ENCUESTA	,
        PERIODO_CARGA	
        FROM {PROJECT_ID}.{DATA_SET_PRC}.BD_REPUESTOS_SQL
        """
        
        query_job = client.query(SQL)
        query_job.result()
        
        print(f'Tabla {PROJECT_ID}.{DATA_SET_DATA_MODEL}.Survey actualizada.')
        
        
    def QueryBdServicios(self, client):

        SQL = f"""
        insert into {PROJECT_ID}.{DATA_SET_DATA_MODEL}.Survey
        (
        SurveyID,
        Domain,
        CompanyID,
        CompanyDescription,
        BranchOfficeID,
        BranchOfficeDescription,
        SalesOfficeDescription,
        OperationPlace,
        SurveyDate,
        SurveyEndDate,
        CustomerID,
        CustomerIDDBS,
        CustomerName,
        ModelDescription,
        SerialNumber,
        SurveyContactName,
        SurveyPerson,
        WorkOrderNumber,
        OpeningDate,
        InvoiceDate,
        TransactionDate,
        LastJobDate,
        WorkOrderClosingDate,
        InvoiceID,
        ServiceType,
        CSAType,
        WorkshopGroup,
        WorkOrderPrefix,
        CC_CN,
        WorkOrderPositionType,
        SatisfactionEvaluation,
        BuybackEvaluation,
        RecommendationEvaluation,
        EaseDoingBusinessEvaluation,
        AvailabilityEvaluation,
        AnswerEvaluation,
        CommunicationEvaluation,
        PreparationEvaluation,
        DurationEvaluation,
        TangibleInvoiceEvaluation,
        QualityEvaluation,
        InvoicePunctualityEvaluation,
        InvoiceAccuracyEvaluation,
        SatisfactionComment,
        BuybackComment,
        RecommendationComment,
        EaseDoingBusinessComment,
        AvailabilityComment,
        AnswerComment,
        CommunicationComment,
        PreparationComment,
        DurationComment,
        TangibleInvoiceComment,
        QualityComment,
        InvoicePunctualityComment,
        InvoiceAccuracyComment,
        HigherRatingComment,
        AdditionalComment,
        RegionDescription,
        ResponsibleManagement,
        SalesRepGroupDescription,
        CustomerSegmentDescription,
        AditionalCustomerSegmentDescription,
        CustomerMarketDescription,
        RRVV,
        PSSR,
        SurveyCounter,
        LoyaltyIndex,
        NetLoyaltyScoreDescription,
        NetLoyaltyScore,
        CustomerType,
        ParentCustomerName,
        AttendedBy,
        BrandDescription,
        LineDescription,
        SurveyOrigin,
        CSAPlanType,
        Period,
        UniqueID
        )
        SELECT
        NRO_ORDEN_DETRABAJO,
        'SERVICE',
        CASE
            WHEN ORGANIZACION = 'Ferreyros' THEN '2000'
            ELSE NULL
        END,
        ORGANIZACION,
        N_SUCURSAL,
        SUCURSAL,
        OFICINA,
        LUGAR_DE_OPERACION,
        MES_ENCUESTA,
        FECHA_FINALIZACION_LA_ENCUESTA,
        NRO_CLIENTE,
        NRO_CLIENTE,
        NOMBRE_CLIENTE,
        MODELO,
        NUMERO_SERIE,
        CONTACTO_CON_EL_CLIENTE,
        PERSONA_QUE_CONTESTO_LA_ENCUESTA,
        NRO_ORDEN_DETRABAJO,
        FECHA_APERTURA,
        FECHA_FACTURA,
        FECHA_TRANSACCION,
        ULTIMO_TRABAJO,
        CIERRE_LA_ORDEN_TRABAJO,
        NRO_FACTURA,
        TIPO_SERVICIO,
        TIPO_CSA,
        AGRUPACION_TALLERES,
        PREFIJO_LA_OT,
        CC_CN,
        TIPO_CARGO_OT,
        SATISFACCION_GENERAL,
        RECOMPRA,
        RECOMENDACION,
        FACILIDAD_PARA_HACER_NEGOCIOS,
        DISPONIBILIDAD,
        RESPUESTA,
        COMUNICACION,
        PREPARACION,
        DURACION,
        TANGIBLES_FACTURA,
        CALIDAD,
        PUNTUALIDAD_LA_FACTURA,
        PRECISION_LA_FACTURA,
        SATISFACCION_GENERAL_COMS,
        RECOMPRA_COMS,
        RECOMENDACION_COMS,
        FACILIDAD_PARA_HACER_NEGOCIOS_COMS,
        DISPONIBILIDAD_Y_FIABILIDAD_COMS,
        RESPUESTA_COMS,
        COMUNICACION_COMS,
        PREPARACION_COMS,
        DURACION_COMS,
        TANGIBLES_FACTURA_COMS,
        CALIDAD_COMS,
        PUNTUALIDAD_LA_FACTURA_COMS,
        PRECISION_LA_FACTURA_COMS,
        MAYOR_VALORACION_COMS,
        COMENTARIOS_ADICIONALES,
        REGION,
        GERENCIA_RESPONSABLE,
        UNIDAD_NEGOCIOS,
        SEGMENTO,
        SEGMENTO_2,
        MERCADO,
        RRVV,
        CASE
            WHEN PSSR = 'nan' THEN null
            else PSSR
        END,
        NRO_ENCUESTAS,
        CAST(INDICE_LEALTAD AS NUMERIC),
        NLS2,
        CAST(NLS AS NUMERIC),
        TIPO_RAZON_SOCIAL,
        RAZON_SOCIAL_PARENT,
        ATENDIDO_POR,
        MARCA,
        LINEA,
        ORIGEN_ENCUESTA,
        TIPO_CSA,
        PERIODO_CARGA,
        identificador_unico	
        FROM {PROJECT_ID}.{DATA_SET_PRC}.BD_SERVICIOS_SQL
        """
        
        query_job = client.query(SQL)
        query_job.result()
        
        print(f'Tabla {PROJECT_ID}.{DATA_SET_DATA_MODEL}.Survey actualizada.')
        
        
        
        
        
    def QueryBdMotivosRep(self, client):
        
        SQL = f"""
        insert into {PROJECT_ID}.{DATA_SET_DATA_MODEL}.ReasonDissatisfaction 
        (
        SurveyID,
        CompanyID,
        CompanyDescription,
        Domain,
        CustomerID,
        CustomerIDDBS,
        Period,
        ReasonDissatisfaction,
        OpportunityImprovement,
        Suggestion
        )
        SELECT
        CASE
            WHEN DOCUMENTO_REFERENCIA = 'None' THEN null
            ELSE DOCUMENTO_REFERENCIA
        END,
        CASE
            WHEN ORGANIZACION = 'Ferreyros' THEN '2000'
            ELSE NULL
        END,
        CASE
            WHEN ORGANIZACION = 'None' THEN null
            else ORGANIZACION
        END,
        'PARTS',
        CASE
            WHEN NUMERO_CLIENTE = 'None' THEN null
            else NUMERO_CLIENTE
        END,
        CASE
            WHEN NUMERO_CLIENTE = 'None' THEN null
            else NUMERO_CLIENTE
        END,
        (concat(SUBSTRING(CAST(MES_ENCUESTA AS STRING), 1, 10))),
        CASE
            WHEN MOTIVO_INSATISFACCION = 'None' THEN null
            else MOTIVO_INSATISFACCION
        END,
        CASE
            WHEN OPORTUNIDAD_MEJORA = 'None' THEN null
            else OPORTUNIDAD_MEJORA
        END,
        CASE
            WHEN SUGERENCIA_MEJORA = 'None' THEN null
            else SUGERENCIA_MEJORA
        END
        FROM {PROJECT_ID}.{DATA_SET_PRC}.BD_MOTIVOS_REP_SQL 
        """
        
        query_job = client.query(SQL)
        query_job.result()
        
        print(f'Tabla {PROJECT_ID}.{DATA_SET_DATA_MODEL}.Survey actualizada.')
        
    
    def QueryBdMotivosPrime(self, client):
        
        SQL = f"""
        insert into {PROJECT_ID}.{DATA_SET_DATA_MODEL}.ReasonDissatisfaction 
        (
        SurveyID,
        CompanyID,
        CompanyDescription,
        Domain,
        CustomerID,
        CustomerIDDBS,
        Period,
        ReasonDissatisfaction,
        OpportunityImprovement,
        Suggestion
        )
        SELECT
        CASE
            WHEN REPORTE_DE_ASIGNACION = 'None' THEN null
            else REPORTE_DE_ASIGNACION
        END, 
        CASE
            WHEN ORGANIZACION = 'Ferreyros' THEN '2000'
            ELSE NULL
        END,
        CASE
            WHEN ORGANIZACION = 'None' THEN null
            else ORGANIZACION
        END,
        'PRIME',
        CASE
            WHEN CODIGO_DE_CLIENTE = 'None' THEN null
            else CODIGO_DE_CLIENTE
        END,
        CASE
            WHEN CODIGO_DE_CLIENTE = 'None' THEN null
            else CODIGO_DE_CLIENTE
        END,
        (concat(SUBSTRING(CAST(MES_ENCUESTA AS STRING), 1, 10))),
        CASE
            WHEN MOTIVOS_DE_INSATISFACCION = 'None' THEN null
            else MOTIVOS_DE_INSATISFACCION
        END,
        OPORTUNIDAD_DE_MEJORA,
        SUGERENCIA,
        FROM {PROJECT_ID}.{DATA_SET_PRC}.BD_MOTIVOS_PRIME_SQL 
        """
        
        query_job = client.query(SQL)
        query_job.result()
        
        print(f'Tabla {PROJECT_ID}.{DATA_SET_DATA_MODEL}.Survey actualizada.')
        
        
    def QueryBdMotivosRental(self, client):
        
        SQL = f"""
        insert into {PROJECT_ID}.{DATA_SET_DATA_MODEL}.ReasonDissatisfaction 
        (
        SurveyID,
        CompanyID,
        CompanyDescription,
        Domain,
        CustomerID,
        CustomerIDDBS,
        Period,
        ReasonDissatisfaction,
        OpportunityImprovement,
        Suggestion
        )
        SELECT
        CAST(EXTRACT(MONTH FROM DATE (MES_ENCUESTA)) AS STRING) || '-' || NUMERO_CLIENTE || '-' || Serie,
        CASE
            WHEN ORGANIZACION = 'Ferreyros' THEN '2000'
            ELSE NULL
        END,
        CASE
            WHEN ORGANIZACION = 'None' THEN null
            else ORGANIZACION
        END,
        'RENTAL',
        CASE
            WHEN NUMERO_CLIENTE = 'None' THEN null
            else NUMERO_CLIENTE
        END,
        CASE
            WHEN NUMERO_CLIENTE = 'None' THEN null
            else NUMERO_CLIENTE
        END,
        (concat(SUBSTRING(CAST(MES_ENCUESTA AS STRING), 1, 10))),
        CASE
            WHEN MOTIVO_INSATISFACCION = 'None' THEN null
            else MOTIVO_INSATISFACCION
        END,
        OPORTUNIDAD_MEJORA,
        SUGERENCIA_MEJORA,
        FROM {PROJECT_ID}.{DATA_SET_PRC}.BD_MOTIVOS_REN_SQL 
        """
        
        query_job = client.query(SQL)
        query_job.result()
        
        print(f'Tabla {PROJECT_ID}.{DATA_SET_DATA_MODEL}.Survey actualizada.')
        
        
    def QueryBdMotivosService(self, client):
        
        SQL = f"""
        insert into {PROJECT_ID}.{DATA_SET_DATA_MODEL}.ReasonDissatisfaction 
        (
        SurveyID,
        CompanyID,
        CompanyDescription,
        Domain,
        CustomerID,
        CustomerIDDBS,
        Period,
        ReasonDissatisfaction,
        OpportunityImprovement,
        Suggestion
        )
        SELECT
        CASE
            WHEN NRO_ORDEN_DETRABAJO = 'None' THEN null
            ELSE NRO_ORDEN_DETRABAJO
        END,
        CASE
            WHEN ORGANIZACION = 'Ferreyros' THEN '2000'
            ELSE NULL
        END,
        CASE
            WHEN ORGANIZACION = 'None' THEN null
            else ORGANIZACION
        END,
        'SERVICE',
        CASE
            WHEN NRO_CLIENTE = 'None' THEN null
            else NRO_CLIENTE
        END,
        CASE
            WHEN NRO_CLIENTE = 'None' THEN null
            else NRO_CLIENTE
        END,
        (concat(SUBSTRING(CAST(MES_ENCUESTA AS STRING), 1, 10))),
        CASE
            WHEN MOTIVO_INSATISFACCION = 'None' THEN null
            else MOTIVO_INSATISFACCION
        END,
        OPORTUNIDAD_MEJORA,
        SUGERENCIA_MEJORA,
        FROM {PROJECT_ID}.{DATA_SET_PRC}.BD_MOTIVOS_SERV_SQL 
        """
        
        query_job = client.query(SQL)
        query_job.result()
        
        print(f'Tabla {PROJECT_ID}.{DATA_SET_DATA_MODEL}.Survey actualizada.')
        
        

qdt = QueriesDataModel()

qdt.QueryBdPrime(client)