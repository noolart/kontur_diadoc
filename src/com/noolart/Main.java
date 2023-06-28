package com.noolart;

import Diadoc.Api.DiadocApi;
import Diadoc.Api.Proto.Documents.DocumentListProtos;
import Diadoc.Api.Proto.Documents.DocumentProtos;
import Diadoc.Api.Proto.Events.DiadocMessage_GetApiProtos;
import Diadoc.Api.Proto.Events.DiadocMessage_PostApiProtos;
import Diadoc.Api.document.DocumentsFilter;
import Diadoc.Api.exceptions.DiadocSdkException;
import com.google.protobuf.ByteString;
import oracle.jdbc.OracleDriver;


import java.io.*;
import java.sql.*;

import java.util.ArrayList;
import java.util.List;


public class Main {


    private static DiadocApi api;
    static final String DefaultApiUrl = "https://diadoc-api.kontur.ru/";
    static final String DefaultClientId = "ключ";
    static String boxId;
    static Statement statement;
    static ArrayList<String> documents,rns;


    public static void main(String[] args) throws DiadocSdkException, SQLException, ClassNotFoundException, IOException {

        DriverManager.registerDriver(new OracleDriver());
        Class.forName("oracle.jdbc.driver.OracleDriver");

        Connection conn = DriverManager.getConnection
                ("url бд", "пользователь бд", "пароль");

        statement = conn.createStatement();

        api = new DiadocApi(DefaultClientId, DefaultApiUrl);
        api.getAuthClient().authenticate("логин диадок", "пароль");
        boxId = api.getOrganizationClient().getMyOrganizations().getOrganizations(0).getBoxes(0).getBoxId();


        documents = new ArrayList<>();
        rns = new ArrayList<>();





        Thread run = new Thread(() -> {
            while (true) {
                try {

                    ResultSet rs = statement.executeQuery("select XMLDOC, rn from aa_t_diadoc_out_queue where status is null or status = 0");
                    //ResultSet rs = statement.executeQuery("select XMLDOC, rn from aa_t_diadoc_out_queue");
                    while (rs.next()) {
                        documents.add((new BufferedReader(rs.getClob(1).getCharacterStream())).readLine());
                        rns.add(rs.getBigDecimal(2).toString());
                    }

                    while (documents.size() != 0) {
                        sendXML(rns.remove(0), documents.remove(0));
                    }

                    Thread.sleep(5*60*1000);

                } catch (SQLException | IOException | DiadocSdkException | InterruptedException exception) {
                    exception.printStackTrace();
                }
            }
        });

        run.start();


    }




    public static void sendXML(String rn, String XML) throws IOException, DiadocSdkException, SQLException {

        String myOrgId = api.getOrganizationClient().getMyOrganizations().getOrganizations(0).getOrgId();
        String orgId = api.getCounteragentClient().getCounteragents(myOrgId, null, null).getCounteragents(0).getOrganization().getOrgId();
        String fromBoxId = api.getOrganizationClient().getOrganizationById(myOrgId).getBoxes(0).getBoxId();
        String toBoxId = api.getOrganizationClient().getOrganizationById(orgId).getBoxes(0).getBoxId();

        byte[] bytes = XML.getBytes("windows-1251");

        ByteString signature = api.getOrganizationClient().getMyOrganizations().getOrganizations(0).getCertificateOfRegistryInfoBytes();
        String shelfId = api.getShelfClient().uploadFileToShelf(bytes);




        DiadocMessage_PostApiProtos.MessageToPost.Builder messageBuilder = DiadocMessage_PostApiProtos.MessageToPost.newBuilder();


        DiadocMessage_PostApiProtos.XmlDocumentAttachment.Builder attachmentBuilder = messageBuilder.addUniversalTransferDocumentSellerTitlesBuilder();
        DiadocMessage_PostApiProtos.SignedContent.Builder signedContentBuilder = DiadocMessage_PostApiProtos.SignedContent.newBuilder();

        signedContentBuilder.setNameOnShelf(shelfId);
        signedContentBuilder.setSignWithTestSignature(true);
        signedContentBuilder.setSignature(signature);

        attachmentBuilder.setSignedContent(signedContentBuilder.build());

        messageBuilder.setFromBoxId(fromBoxId);
        messageBuilder.setToBoxId(toBoxId);


        try {

            DiadocMessage_GetApiProtos.Message response = api.getMessageClient().postMessage(messageBuilder.build());
            //System.out.println(response);
            statement.executeUpdate ("UPDATE aa_t_diadoc_out_queue  set result='" + "sent" + "', status=1 where rn=" + rn);
        }
        catch (Exception e) {
            //System.out.println("declare str varchar2(32767); begin str :='" + e.toString().replace("'", "\"") + "';  UPDATE aa_t_diadoc_out_queue  set result=str, status=-1 where rn=" + rn +"; end;");
            statement.executeUpdate ("declare str varchar2(32767); begin str :='" + e.toString().replace("'", "") + "';  UPDATE aa_t_diadoc_out_queue  set result=str, status=-1 where rn=" + rn +"; end;");

        }
    }


    public static List<DocumentProtos.Document> getInboundDocumentList() throws DiadocSdkException {

        DocumentsFilter docFilter = new DocumentsFilter();

        /*Обязательный параметр filterCategory задается строкой в формате «[DocumentType].[DocumentClass][DocumentStatus]».

        * Первая часть этой строки задает тип документа и может принимать либо одно из значений перечисления DocumentType, либо одно из специальных значений:

        • AnyInvoiceDocumentType - соответствует набору из четырех типов документов СФ/ИСФ/КСФ/ИКСФ (Invoice, InvoiceRevision, InvoiceCorrection, InvoiceCorrectionRevision),
        • AnyBilateralDocumentType - соответствует любому типу двусторонних документов (Nonformalized, Torg12, AcceptanceCertificate, XmlTorg12, XmlAcceptanceCertificate, TrustConnectionRequest, PriceList, PriceListAgreement, CertificateRegistry, ReconciliationAct, Contract, Torg13),
        • AnyUnilateralDocumentType - соответствует любому типу односторонних документов (ProformaInvoice, ServiceDetails),
        • Any - соответствует любому типу документа.

        Строка DocumentClass задает класс документа и может принимать следующие значения:

        • Inbound (входящий документ),
        • Outbound (исходящий документ),
        • Internal (внутренний документ),
        • Proxy (документ, переданный через промежуточного получателя)

        Строка DocumentStatus задает статус документа и может принимать следующие значения:

        • Пустое значение (любой документ указанного класса Class),
        • NotRead (документ не прочитан),
        • NoRecipientSignatureRequest (документ без запроса ответной подписи),
        • WaitingForRecipientSignature (документ в ожидании ответной подписи),
        • WithRecipientSignature (документ с ответной подписью),
        • WithSenderSignature (документ с подписью отправителя),
        • RecipientSignatureRequestRejected (документ с отказом от формирования ответной подписи),
        • WaitingForSenderSignature (документ, требующий подписания и отправки),
        • InvalidSenderSignature (документ с невалидной подписью отправителя, требующий повторного подписания и отправки),
        • InvalidRecipientSignature (документ с невалидной подписью получателя, требующий повторного подписания и отправки),
        • Approved (согласованный документ),
        • Disapproved (документ с отказом согласования),
        • WaitingForResolution (документ, находящийся на согласовании или подписи),
        • SignatureRequestRejected (документ с отказом в запросе подписи сотруднику),
        • Finished (документ с завершенным документооборотом),
        • HaveToCreateReceipt (требуется подписать извещение о получении),
        • NotFinished (документ с незавершенным документооборотом),
        • InvoiceAmendmentRequested (имеет смысл только для счетов-фактур; документ, по которому было запрошено уточнение),
        • RevocationIsRequestedByMe (документ, по которому было запрошено аннулирование),
        • RequestsMyRevocation (документ, по которому контрагент запросил аннулирование),
        • RevocationAccepted (аннулированный документ),
        • RevocationRejected (документ, запрос на аннулирование которого был отклонен),
        • RevocationApproved (документ, запрос на аннулирование которого был согласован),
        • RevocationDisapproved (документ с отказом согласования запроса на аннулирование),
        • WaitingForRevocationApprovement (документ, находящийся на согласовании запроса аннулирования),
        • NotRevoked (неаннулированный документ)
        • WaitingForProxySignature (документ в ожидании подписи промежуточного получателя),
        • WithProxySignature (документ с подписью промежуточного получателя),
        • InvalidProxySignature (документ с невалидной подписью промежуточного получателя, требующий повторного подписания и отправки),
        • ProxySignatureRejected (документ с отказом от формирования подписи промежуточным получателем),
        • WaitingForInvoiceReceipt (документ в ожидании получения извещения о получении счетафактуры),
        • WaitingForReceipt (документ в ожидании получения извещения о получении),
        • RequestsMySignature (документ, по которому контрагент запросил подпись),
        • RoamingNotificationError (документ, с ошибкой доставки в роуминге)

        Примеры строки filterCategory:

        • AnyUnilateralDocumentType.InboundNotRevoked (все входящие односторонние неаннулированные
        документы),
        • XmlTorg12.OutboundWithRecipientSignature (все исходящие формализованные ТОРГ-12, подписанные контрагентом),
        • InvoiceCorrection.OutboundInvoiceAmendmentRequested (все исходящие КСФ, по которым контрагент запросил уточнение).

        */

        //TODO установите необходимый фильтр в строке ниже

        docFilter.setFilterCategory("Any.Inbound");
        docFilter.setBoxId(boxId);

        DocumentListProtos.DocumentList documentList =  api.getDocumentClient().getDocuments(docFilter);

        return documentList.getDocumentsList();
    }

}






