// ============================================================================
//
// Copyright (C) 2006-2017 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================

package org.talend.components.netsuite.v2014_2.client;

import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.SocketException;
import java.net.URL;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.xml.ws.BindingProvider;
import javax.xml.ws.WebServiceException;
import javax.xml.ws.WebServiceFeature;
import javax.xml.ws.soap.SOAPFaultException;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.cxf.feature.LoggingFeature;
import org.talend.components.netsuite.NetSuiteErrorCode;
import org.talend.components.netsuite.client.CustomMetaDataSource;
import org.talend.components.netsuite.client.EmptyCustomMetaDataSource;
import org.talend.components.netsuite.client.NetSuiteClientService;
import org.talend.components.netsuite.client.NetSuiteCredentials;
import org.talend.components.netsuite.client.NetSuiteException;
import org.talend.components.netsuite.client.NsPreferences;
import org.talend.components.netsuite.client.NsReadResponse;
import org.talend.components.netsuite.client.NsSearchPreferences;
import org.talend.components.netsuite.client.NsSearchResult;
import org.talend.components.netsuite.client.NsStatus;
import org.talend.components.netsuite.client.NsWriteResponse;
import org.talend.components.netsuite.client.model.BasicMetaData;
import org.talend.components.netsuite.v2014_2.client.model.BasicMetaDataImpl;

import com.netsuite.webservices.v2014_2.platform.ExceededRequestSizeFault;
import com.netsuite.webservices.v2014_2.platform.InvalidCredentialsFault;
import com.netsuite.webservices.v2014_2.platform.InvalidSessionFault;
import com.netsuite.webservices.v2014_2.platform.NetSuitePortType;
import com.netsuite.webservices.v2014_2.platform.NetSuiteService;
import com.netsuite.webservices.v2014_2.platform.UnexpectedErrorFault;
import com.netsuite.webservices.v2014_2.platform.core.BaseRef;
import com.netsuite.webservices.v2014_2.platform.core.DataCenterUrls;
import com.netsuite.webservices.v2014_2.platform.core.Passport;
import com.netsuite.webservices.v2014_2.platform.core.Record;
import com.netsuite.webservices.v2014_2.platform.core.RecordRef;
import com.netsuite.webservices.v2014_2.platform.core.SearchRecord;
import com.netsuite.webservices.v2014_2.platform.core.SearchResult;
import com.netsuite.webservices.v2014_2.platform.core.Status;
import com.netsuite.webservices.v2014_2.platform.core.StatusDetail;
import com.netsuite.webservices.v2014_2.platform.messages.AddListRequest;
import com.netsuite.webservices.v2014_2.platform.messages.AddRequest;
import com.netsuite.webservices.v2014_2.platform.messages.ApplicationInfo;
import com.netsuite.webservices.v2014_2.platform.messages.DeleteListRequest;
import com.netsuite.webservices.v2014_2.platform.messages.DeleteRequest;
import com.netsuite.webservices.v2014_2.platform.messages.GetDataCenterUrlsRequest;
import com.netsuite.webservices.v2014_2.platform.messages.GetDataCenterUrlsResponse;
import com.netsuite.webservices.v2014_2.platform.messages.GetRequest;
import com.netsuite.webservices.v2014_2.platform.messages.LoginRequest;
import com.netsuite.webservices.v2014_2.platform.messages.LoginResponse;
import com.netsuite.webservices.v2014_2.platform.messages.LogoutRequest;
import com.netsuite.webservices.v2014_2.platform.messages.Preferences;
import com.netsuite.webservices.v2014_2.platform.messages.ReadResponse;
import com.netsuite.webservices.v2014_2.platform.messages.ReadResponseList;
import com.netsuite.webservices.v2014_2.platform.messages.SearchMoreRequest;
import com.netsuite.webservices.v2014_2.platform.messages.SearchMoreWithIdRequest;
import com.netsuite.webservices.v2014_2.platform.messages.SearchNextRequest;
import com.netsuite.webservices.v2014_2.platform.messages.SearchPreferences;
import com.netsuite.webservices.v2014_2.platform.messages.SearchRequest;
import com.netsuite.webservices.v2014_2.platform.messages.SessionResponse;
import com.netsuite.webservices.v2014_2.platform.messages.UpdateListRequest;
import com.netsuite.webservices.v2014_2.platform.messages.UpdateRequest;
import com.netsuite.webservices.v2014_2.platform.messages.UpsertListRequest;
import com.netsuite.webservices.v2014_2.platform.messages.UpsertRequest;
import com.netsuite.webservices.v2014_2.platform.messages.WriteResponse;
import com.netsuite.webservices.v2014_2.platform.messages.WriteResponseList;

/**
 *
 */
public class NetSuiteClientServiceImpl extends NetSuiteClientService<NetSuitePortType> {

    public static final String DEFAULT_ENDPOINT_URL =
            "https://webservices.netsuite.com/services/NetSuitePort_2014_2";

    public static final String NS_URI_PLATFORM_MESSAGES =
            "urn:messages_2014_2.platform.webservices.netsuite.com";

    public NetSuiteClientServiceImpl() {
        super();

        metaDataSource = createDefaultMetaDataSource();
    }

    @Override
    public <RecT, SearchT> NsSearchResult<RecT> search(final SearchT searchRecord) throws NetSuiteException {
        return execute(new PortOperation<NsSearchResult<RecT>, NetSuitePortType>() {
            @Override public NsSearchResult<RecT> execute(NetSuitePortType port) throws Exception {
                SearchRequest request = new SearchRequest();
                SearchRecord sr = (SearchRecord) searchRecord;
                request.setSearchRecord(sr);

                SearchResult result = port.search(request).getSearchResult();
                return toNsSearchResult(result);
            }
        });
    }

    @Override
    public <RecT> NsSearchResult<RecT> searchMore(final int pageIndex) throws NetSuiteException {
        return execute(new PortOperation<NsSearchResult<RecT>, NetSuitePortType>() {
            @Override public NsSearchResult<RecT> execute(NetSuitePortType port) throws Exception {
                SearchMoreRequest request = new SearchMoreRequest();
                request.setPageIndex(pageIndex);

                SearchResult result = port.searchMore(request).getSearchResult();
                return toNsSearchResult(result);
            }
        });
    }

    @Override
    public <RecT> NsSearchResult<RecT> searchMoreWithId(
            final String searchId, final int pageIndex) throws NetSuiteException {
        return execute(new PortOperation<NsSearchResult<RecT>, NetSuitePortType>() {
            @Override public NsSearchResult<RecT> execute(NetSuitePortType port) throws Exception {
                SearchMoreWithIdRequest request = new SearchMoreWithIdRequest();
                request.setSearchId(searchId);
                request.setPageIndex(pageIndex);

                SearchResult result = port.searchMoreWithId(request).getSearchResult();
                return toNsSearchResult(result);
            }
        });
    }

    @Override
    public <RecT> NsSearchResult<RecT> searchNext() throws NetSuiteException {
        return execute(new PortOperation<NsSearchResult<RecT>, NetSuitePortType>() {
            @Override public NsSearchResult<RecT> execute(NetSuitePortType port) throws Exception {
                SearchNextRequest request = new SearchNextRequest();
                SearchResult result = port.searchNext(request).getSearchResult();
                return toNsSearchResult(result);
            }
        });
    }

    @Override
    public <RecT, RefT> NsReadResponse<RecT> get(final RefT ref) throws NetSuiteException {
        if (ref == null) {
            return new NsReadResponse<>();
        }
        return execute(new PortOperation<NsReadResponse<RecT>, NetSuitePortType>() {
            @Override public NsReadResponse<RecT> execute(NetSuitePortType port) throws Exception {
                GetRequest request = new GetRequest();
                request.setBaseRef((BaseRef) ref);

                ReadResponse response = port.get(request).getReadResponse();
                return toNsReadResponse(response);
            }
        });
    }

    @Override
    public <RecT, RefT> NsWriteResponse<RefT> add(final RecT record) throws NetSuiteException {
        if (record == null) {
            return new NsWriteResponse();
        }
        return execute(new PortOperation<NsWriteResponse<RefT>, NetSuitePortType>() {
            @Override public NsWriteResponse<RefT> execute(NetSuitePortType port) throws Exception {
                AddRequest request = new AddRequest();
                request.setRecord((Record) record);

                WriteResponse response = port.add(request).getWriteResponse();
                return toNsWriteResponse(response);
            }
        });
    }

    @Override
    public <RecT, RefT> List<NsWriteResponse<RefT>> addList(final List<RecT> records) throws NetSuiteException {
        if (records == null || records.isEmpty()) {
            return Collections.emptyList();
        }
        return execute(new PortOperation<List<NsWriteResponse<RefT>>, NetSuitePortType>() {
            @Override public List<NsWriteResponse<RefT>> execute(NetSuitePortType port) throws Exception {
                AddListRequest request = new AddListRequest();
                request.getRecord().addAll(toRecordList(records));

                WriteResponseList writeResponseList = port.addList(request).getWriteResponseList();
                return toNsWriteResponseList(writeResponseList);
            }
        });
    }

    @Override
    public <RecT, RefT> NsWriteResponse<RefT> update(final RecT record) throws NetSuiteException {
        if (record == null) {
            return new NsWriteResponse();
        }
        return execute(new PortOperation<NsWriteResponse<RefT>, NetSuitePortType>() {
            @Override public NsWriteResponse<RefT> execute(NetSuitePortType port) throws Exception {
                UpdateRequest request = new UpdateRequest();
                request.setRecord((Record) record);

                WriteResponse response = port.update(request).getWriteResponse();
                return toNsWriteResponse(response);
            }
        });
    }

    @Override
    public <RecT, RefT> List<NsWriteResponse<RefT>> updateList(final List<RecT> records) throws NetSuiteException {
        if (records == null || records.isEmpty()) {
            return Collections.emptyList();
        }
        return execute(new PortOperation<List<NsWriteResponse<RefT>>, NetSuitePortType>() {
            @Override public List<NsWriteResponse<RefT>> execute(NetSuitePortType port) throws Exception {
                UpdateListRequest request = new UpdateListRequest();
                request.getRecord().addAll(toRecordList(records));

                WriteResponseList writeResponseList = port.updateList(request).getWriteResponseList();
                return toNsWriteResponseList(writeResponseList);
            }
        });
    }

    @Override
    public <RecT, RefT> NsWriteResponse<RefT> upsert(final RecT record) throws NetSuiteException {
        if (record == null) {
            return new NsWriteResponse();
        }
        return execute(new PortOperation<NsWriteResponse<RefT>, NetSuitePortType>() {
            @Override public NsWriteResponse<RefT> execute(NetSuitePortType port) throws Exception {
                UpsertRequest request = new UpsertRequest();
                request.setRecord((Record) record);

                WriteResponse response = port.upsert(request).getWriteResponse();
                return toNsWriteResponse(response);
            }
        });
    }

    @Override
    public <RecT, RefT> List<NsWriteResponse<RefT>> upsertList(final List<RecT> records) throws NetSuiteException {
        if (records == null || records.isEmpty()) {
            return Collections.emptyList();
        }
        return execute(new PortOperation<List<NsWriteResponse<RefT>>, NetSuitePortType>() {
            @Override public List<NsWriteResponse<RefT>> execute(NetSuitePortType port) throws Exception {
                UpsertListRequest request = new UpsertListRequest();
                request.getRecord().addAll(toRecordList(records));

                WriteResponseList writeResponseList = port.upsertList(request).getWriteResponseList();
                return toNsWriteResponseList(writeResponseList);
            }
        });
    }

    @Override
    public <RefT> NsWriteResponse<RefT> delete(final RefT ref) throws NetSuiteException {
        if (ref == null) {
            return new NsWriteResponse<>();
        }
        return execute(new PortOperation<NsWriteResponse<RefT>, NetSuitePortType>() {
            @Override public NsWriteResponse execute(NetSuitePortType port) throws Exception {
                DeleteRequest request = new DeleteRequest();
                BaseRef baseRef = (BaseRef) ref;
                request.setBaseRef(baseRef);

                WriteResponse writeResponse = port.delete(request).getWriteResponse();
                return toNsWriteResponse(writeResponse);
            }
        });
    }

    @Override
    public <RefT> List<NsWriteResponse<RefT>> deleteList(final List<RefT> refs) throws NetSuiteException {
        if (refs == null || refs.isEmpty()) {
            return Collections.emptyList();
        }
        return execute(new PortOperation<List<NsWriteResponse<RefT>>, NetSuitePortType>() {
            @Override public List<NsWriteResponse<RefT>> execute(NetSuitePortType port) throws Exception {
                DeleteListRequest request = new DeleteListRequest();
                request.getBaseRef().addAll(toBaseRefList(refs));

                WriteResponseList writeResponseList = port.deleteList(request).getWriteResponseList();
                return toNsWriteResponseList(writeResponseList);
            }
        });
    }

    @Override
    public BasicMetaData getBasicMetaData() {
        return BasicMetaDataImpl.getInstance();
    }

    @Override
    public CustomMetaDataSource createDefaultCustomMetaDataSource() {
        return new EmptyCustomMetaDataSource();
    }

    protected void doLogout() throws NetSuiteException {
        try {
            LogoutRequest request = new LogoutRequest();
            port.logout(request);
        } catch (Exception e) {
            throw new NetSuiteException(e.getMessage(), e);
        }
    }

    protected void doLogin() throws NetSuiteException {
        port = getNetSuitePort(endpointUrl, credentials.getAccount());

        setHttpClientPolicy(port);

        setLoginHeaders(port);

        PortOperation<SessionResponse, NetSuitePortType> loginOp;
        if (!credentials.isUseSsoLogin()) {
            final Passport passport = createNativePassport(credentials);
            loginOp = new PortOperation<SessionResponse, NetSuitePortType>() {
                @Override public SessionResponse execute(NetSuitePortType port) throws Exception {
                    LoginRequest request = new LoginRequest();
                    request.setPassport(passport);
                    LoginResponse response = port.login(request);
                    return response.getSessionResponse();
                }
            };
        } else {
            throw new NetSuiteException("SSO login not supported");
        }

        Status status = null;
        SessionResponse sessionResponse;
        String exceptionMessage = null;
        for (int i = 0; i < getRetryCount(); i++) {
            try {
                sessionResponse = loginOp.execute(port);
                status = sessionResponse.getStatus();

            } catch (InvalidCredentialsFault f) {
                throw new NetSuiteException(new NetSuiteErrorCode("CLIENT_ERROR"), f.getFaultInfo().getMessage());
            } catch (UnexpectedErrorFault f) {
                exceptionMessage = f.getFaultInfo().getMessage();
            } catch (Exception e) {
                exceptionMessage = e.getMessage();
            }

            if (status != null) {
                break;
            }

            if (i != getRetryCount() - 1) {
                waitForRetryInterval();
            }
        }

        if (status == null || !status.getIsSuccess()) {
            String message = "Login Failed:";
            if (status != null && status.getStatusDetail() != null && status.getStatusDetail().size() > 0) {
                message = message + " " + status.getStatusDetail().get(0).getCode();
                message = message + " " + status.getStatusDetail().get(0).getMessage();
            } else if (exceptionMessage != null) {
                message = message + " " + exceptionMessage;
            }

            throw new NetSuiteException(new NetSuiteErrorCode("CLIENT_ERROR"), message);
        }

        remoteLoginHeaders(port);
    }

    @Override
    protected String getPlatformMessageNamespaceUri() {
        return NS_URI_PLATFORM_MESSAGES;
    }

    @Override
    protected Preferences createNativePreferences(NsPreferences nsPreferences) {
        Preferences preferences = new Preferences();
        try {
            BeanUtils.copyProperties(preferences, nsPreferences);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new NetSuiteException(e.getMessage(), e);
        }
        return preferences;
    }

    @Override
    protected SearchPreferences createNativeSearchPreferences(NsSearchPreferences nsSearchPreferences) {
        SearchPreferences searchPreferences = new SearchPreferences();
        try {
            BeanUtils.copyProperties(searchPreferences, nsSearchPreferences);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new NetSuiteException(e.getMessage(), e);
        }
        return searchPreferences;
    }

    @Override
    protected ApplicationInfo createNativeApplicationInfo(NetSuiteCredentials credentials) {
        ApplicationInfo applicationInfo = new ApplicationInfo();
        applicationInfo.setApplicationId(credentials.getApplicationId());
        return applicationInfo;
    }

    @Override
    protected Passport createNativePassport(NetSuiteCredentials nsCredentials) {
        RecordRef roleRecord = new RecordRef();
        roleRecord.setInternalId(nsCredentials.getRoleId());

        final Passport passport = new Passport();
        passport.setEmail(nsCredentials.getEmail());
        passport.setPassword(nsCredentials.getPassword());
        passport.setRole(roleRecord);
        passport.setAccount(nsCredentials.getAccount());

        return passport;
    }

    protected NetSuitePortType getNetSuitePort(String defaultEndpointUrl, String account) throws NetSuiteException {
        try {
            URL wsdlLocationUrl = this.getClass().getResource("/wsdl/2014.2/netsuite.wsdl");

            NetSuiteService service = new NetSuiteService(wsdlLocationUrl, NetSuiteService.SERVICE);

            List<WebServiceFeature> features = new ArrayList<>(2);
            if (isMessageLoggingEnabled()) {
                features.add(new LoggingFeature());
            }
            NetSuitePortType port = service.getNetSuitePort(
                    features.toArray(new WebServiceFeature[features.size()]));

            BindingProvider provider = (BindingProvider) port;
            Map<String, Object> requestContext = provider.getRequestContext();
            requestContext.put(BindingProvider.ENDPOINT_ADDRESS_PROPERTY, defaultEndpointUrl);

            GetDataCenterUrlsRequest dataCenterRequest = new GetDataCenterUrlsRequest();
            dataCenterRequest.setAccount(account);
            DataCenterUrls urls = null;
            GetDataCenterUrlsResponse response = port.getDataCenterUrls(dataCenterRequest);
            if (response != null && response.getGetDataCenterUrlsResult() != null) {
                urls = response.getGetDataCenterUrlsResult().getDataCenterUrls();
            }
            if (urls == null) {
                throw new NetSuiteException(new NetSuiteErrorCode("CLIENT_ERROR"),
                        "Can't get a correct webservice domain! "
                                + "Please check your configuration or try to run again.");
            }

            String wsDomain = urls.getWebservicesDomain();
            String endpointUrl = wsDomain.concat(new URL(defaultEndpointUrl).getPath());

            requestContext.put(BindingProvider.SESSION_MAINTAIN_PROPERTY, true);
            requestContext.put(BindingProvider.ENDPOINT_ADDRESS_PROPERTY, endpointUrl);

            return port;
        } catch (WebServiceException | MalformedURLException |
                UnexpectedErrorFault | ExceededRequestSizeFault e) {
            throw new NetSuiteException(new NetSuiteErrorCode("CLIENT_ERROR"),
                    "Failed to get NetSuite port due to error", e);
        }
    }

    @Override
    protected boolean errorCanBeWorkedAround(Throwable t) {
        if (t instanceof InvalidSessionFault ||
                t instanceof RemoteException ||
                t instanceof SOAPFaultException ||
                t instanceof SocketException)
            return true;

        return false;
    }

    @Override
    protected boolean errorRequiresNewLogin(Throwable t) {
        if (t instanceof InvalidSessionFault || t instanceof SocketException) {
            return true;
        }
        return false;
    }

    public static <RefT> List<NsWriteResponse<RefT>> toNsWriteResponseList(WriteResponseList writeResponseList) {
        List<NsWriteResponse<RefT>> nsWriteResponses = new ArrayList<>(writeResponseList.getWriteResponse().size());
        for (WriteResponse writeResponse : writeResponseList.getWriteResponse()) {
            nsWriteResponses.add((NsWriteResponse<RefT>) toNsWriteResponse(writeResponse));
        }
        return nsWriteResponses;
    }

    public static <RecT> List<NsReadResponse<RecT>> toNsReadResponseList(ReadResponseList readResponseList) {
        List<NsReadResponse<RecT>> nsReadResponses = new ArrayList<>(readResponseList.getReadResponse().size());
        for (ReadResponse readResponse : readResponseList.getReadResponse()) {
            nsReadResponses.add((NsReadResponse<RecT>) toNsReadResponse(readResponse));
        }
        return nsReadResponses;
    }

    public static <RecT> NsSearchResult<RecT> toNsSearchResult(SearchResult result) {
        NsSearchResult nsResult = new NsSearchResult(toNsStatus(result.getStatus()));
        nsResult.setSearchId(result.getSearchId());
        nsResult.setTotalPages(result.getTotalPages());
        nsResult.setTotalRecords(result.getTotalRecords());
        nsResult.setPageIndex(result.getPageIndex());
        nsResult.setPageSize(result.getPageSize());
        if (result.getRecordList() != null) {
            List<Record> nsRecordList = new ArrayList<>(result.getRecordList().getRecord().size());
            for (Record record : result.getRecordList().getRecord()) {
                nsRecordList.add(record);
            }
            nsResult.setRecordList(nsRecordList);
        } else {
            nsResult.setRecordList(Collections.emptyList());
        }
        return nsResult;
    }

    public static <RefT> NsWriteResponse<RefT> toNsWriteResponse(WriteResponse writeResponse) {
        NsWriteResponse<RefT> nsWriteResponse = new NsWriteResponse(
                toNsStatus(writeResponse.getStatus()),
                writeResponse.getBaseRef());
        return nsWriteResponse;
    }

    public static <RecT> NsReadResponse<RecT> toNsReadResponse(ReadResponse readResponse) {
        NsReadResponse<RecT> nsReadResponse = new NsReadResponse(
                toNsStatus(readResponse.getStatus()),
                readResponse.getRecord());
        return nsReadResponse;
    }

    public static <RecT> List<Record> toRecordList(List<RecT> nsRecordList) {
        List<Record> recordList = new ArrayList<>(nsRecordList.size());
        for (RecT nsRecord : nsRecordList) {
            Record r = (Record) nsRecord;
            recordList.add(r);
        }
        return recordList;
    }

    public static <RefT> List<BaseRef> toBaseRefList(List<RefT> nsRefList) {
        List<BaseRef> baseRefList = new ArrayList<>(nsRefList.size());
        for (RefT nsRef : nsRefList) {
            BaseRef r = (BaseRef) nsRef;
            baseRefList.add(r);
        }
        return baseRefList;
    }

    public static NsStatus toNsStatus(Status status) {
        NsStatus nsStatus = new NsStatus();
        nsStatus.setSuccess(status.getIsSuccess());
        List<NsStatus.Detail> details = new ArrayList<>();
        for (StatusDetail detail : status.getStatusDetail()) {
            details.add(toNsStatusDetail(detail));
        }
        return nsStatus;
    }

    public static NsStatus.Detail toNsStatusDetail(StatusDetail detail) {
        NsStatus.Detail nsDetail = new NsStatus.Detail();
        nsDetail.setType(NsStatus.Type.valueOf(detail.getType().value()));
        nsDetail.setCode(detail.getCode().value());
        nsDetail.setMessage(detail.getMessage());
        return nsDetail;
    }
}
