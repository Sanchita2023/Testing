package com.mu.api.domain.processor.impl;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mu.api.domain.constants.MUConstants;
import com.mu.api.domain.service.ExactTargetService;
import com.mu.api.domain.service.GigyaService;
import com.mu.api.platform.exception.BadRequestRuntimeException;
import com.mu.api.platform.exception.BaseServiceRuntimeException;
import com.mu.api.platform.exception.DependentServiceRuntimeException;
import com.mu.api.platform.exception.UnexpectedErrorRuntimeException;
import com.mu.api.utils.EncryptDecrypt;

/**
 * Processor Implementation Class for ParentConsentProcessor.
 * 
 * @author shivani.chopra
 */
public class ParentConsentProcessor extends AbstractRequestProcessorWithPrePostChain {

	/**
	 * LOGGER instance of ParentConsentProcessor.
	 */
	private static final Logger LOGGER = Logger.getLogger(ParentConsentProcessor.class);
	
	/**
	 * Instance of gigyaService dependency.
	 */
	private GigyaService gigyaService;
	
	/**
	 * Instance of exactTargetService exactTargetService.
	 */
	private ExactTargetService exactTargetService;
	
	/**
	 * Instance of solrListPaginationKeys.
	 */
	private String encryptionKey;
	
	/**
	 * Instance of EncryptDecrypt dependency.
	 */
	private EncryptDecrypt encryptDecrypt;
	
	/**
	 * @param input instance of JSONObject
	 * @return output instance of JSONObject after sending email to parent and setting isActive as false
	 * @throws ParseException exception
	 * @throws IOException exception
	 */
	@SuppressWarnings({ "unchecked", "deprecation" })
	@Override
	public JSONObject process(final JSONObject input) throws ParseException, IOException {

		if(MUConstants.IS_DEBUG_ENABLED) {
			LOGGER.debug("Inside ParentConsentProcessor process : " + input);
		}
		
		JSONObject responseObj = new JSONObject();
		
		try {
		
			if (input.containsKey(MUConstants.REQUEST_BODY)) {
				
				JSONObject requestBodyObj = (JSONObject) input.get(MUConstants.REQUEST_BODY);
				if(MUConstants.IS_DEBUG_ENABLED) {
					LOGGER.debug("TrackParentConsent - request : " + requestBodyObj);
				}
				
				if (requestBodyObj.containsKey(MUConstants.GIGYA_UID)
						&& requestBodyObj.containsKey(MUConstants.PARENT_FIRST_NAME) 
						&& requestBodyObj.containsKey(MUConstants.PARENT_LAST_NAME)
						&& requestBodyObj.containsKey(MUConstants.PARENT_EMAIL_ID)
						&& requestBodyObj.containsKey(MUConstants.PARENT_RELATION)
						&& requestBodyObj.containsKey(MUConstants.U_13_NAME)
						&& requestBodyObj.containsKey(MUConstants.GIGYA_UID)) {
					
					String uId = (String) requestBodyObj.get(MUConstants.GIGYA_UID);
					String sParentFirstName = (String) requestBodyObj.get(MUConstants.PARENT_FIRST_NAME);
					String sParentLastName = (String) requestBodyObj.get(MUConstants.PARENT_LAST_NAME);
					String sRelation = (String) requestBodyObj.get(MUConstants.PARENT_RELATION);
					String sToMailId = (String) requestBodyObj.get(MUConstants.PARENT_EMAIL_ID);	
					boolean retryParentConsent = false;
					if (requestBodyObj.containsKey(MUConstants.RETRY_PARENT_CONSENT)) {
						 retryParentConsent = (boolean) requestBodyObj.get(MUConstants.RETRY_PARENT_CONSENT);
					}
				
					if(MUConstants.IS_DEBUG_ENABLED) {
						LOGGER.debug("uID:" + uId);
						LOGGER.debug("sParentFirstName:" + sParentFirstName);
						LOGGER.debug("sParentLastName:" + sParentLastName);
						LOGGER.debug("sRelation:" + sRelation);					
						LOGGER.debug("TrackParentConsent sToMailId : " + sToMailId);
					}
					
					String locale = "en";
					if (requestBodyObj.containsKey(MUConstants.LOCALE_S)) {
						locale = (String) requestBodyObj.get(MUConstants.LOCALE_S);
					}
					locale = locale.length() > 2 ? locale.substring(0, 2) : locale;
					boolean bSetParentDetails = false;
					
					if(retryParentConsent) {
						bSetParentDetails = true;
					} else {
						 bSetParentDetails = setParentDetails(input, uId, sParentFirstName, sParentLastName, sToMailId, sRelation);
					}
					
					if (bSetParentDetails) {
						
						List<String> verificationUrls = generateUrl(input, uId, locale);
						LOGGER.info("TrackParentConsent verificationUrls: " + verificationUrls);
					
						if ((verificationUrls != null) && (!verificationUrls.isEmpty()) && (verificationUrls.size() == 2)) {				
							
							if(MUConstants.IS_DEBUG_ENABLED) {
								LOGGER.debug("VerificationUrls Created Successfully.");
							}
							
							String firstName = (String) requestBodyObj.get(MUConstants.PARENT_FIRST_NAME);
							String lastName = (String) requestBodyObj.get(MUConstants.PARENT_LAST_NAME);
							boolean fFlag = firstName.matches(MUConstants.ENG_REGEX);
							boolean lFlag = lastName.matches(MUConstants.ENG_REGEX);
							if (fFlag) {
								firstName = ("".equals(firstName) ? firstName : firstName.substring(0, 1).toUpperCase())
										+ (firstName.length() > 0 ? firstName.substring(1).toString() : "");
							}
							if (lFlag) {
								lastName = ("".equals(lastName) ? lastName : lastName.substring(0, 1).toUpperCase())
										+ (lastName.length() > 0 ? lastName.substring(1).toString() : "");
							}
							String sParentName = firstName + MUConstants.WIDE_SPACE + lastName;							
							String sU13Name = (String) requestBodyObj.get(MUConstants.U_13_NAME);
							boolean uFlag = sU13Name.matches(MUConstants.ENG_REGEX);
							if (uFlag) {
								sU13Name = ("".equals(sU13Name) ? sU13Name : sU13Name.substring(0, 1).toUpperCase())
										+ (sU13Name.length() > 0 ? sU13Name.substring(1).toString() : "");
							}
							JSONObject data = new JSONObject();
							data.put("acceptLink", verificationUrls.get(0));
							data.put("denyLink", verificationUrls.get(1));
							data.put("sParentName", sParentName);
							data.put("sU13Name", sU13Name);
							JSONObject sMailSentResponse = 
									exactTargetService.sendEmail(input, sToMailId, "parentConsent", data, locale);
							if(MUConstants.IS_DEBUG_ENABLED) {
								LOGGER.debug("TrackParentConsent sendEmail:  sMailToId : " + sToMailId + "event : " + "parentConsent" + " data : " + data + " locale : " + locale + "emailResponse : " + sMailSentResponse);	
							}
							responseObj.put(MUConstants.MAIL_SENT_TO_PARENT, sMailSentResponse);
							JSONObject processResponse = new JSONObject();
							if (sMailSentResponse != null && sMailSentResponse.containsKey("responses")) {
			        	  		JSONArray responseArray = (JSONArray) sMailSentResponse.get("responses");
			        	  		if (responseArray != null && responseArray.size() > 0) {
			        	  			JSONObject response = (JSONObject) responseArray.get(0);
			            	  		if (response.get("hasErrors").toString().equalsIgnoreCase(MUConstants.BOOLEAN_FALSE)) {
			                          processResponse.put(MUConstants.MESSAGE, MUConstants.SUCCESS);
			            	  		} else {
			                          processResponse.put(MUConstants.MESSAGE, MUConstants.FAIL);
			            	  		}
			        	  		} else {
			        	  			processResponse.put(MUConstants.MESSAGE, MUConstants.FAIL);
			        	  		}
				        	} else {					        	  
				        		processResponse.put(MUConstants.MESSAGE, MUConstants.FAIL);
				      	  	}
							
							if(processResponse.get(MUConstants.MESSAGE) == MUConstants.SUCCESS) {
								DateFormat dateformat = new SimpleDateFormat(MUConstants.DATE_TIME_FORMAT, Locale.US);
					          	   JSONObject parentConsentStatus = new JSONObject();
				          	  	    parentConsentStatus.put("ParentConsent", dateformat.format(new Date()));
					          	  	JSONObject actionOnEmail = new JSONObject();
					          	    actionOnEmail.put("ParentConsent", parentConsentStatus);
					          	   JSONObject dataStored = new JSONObject();
					          	 dataStored.put("emails", actionOnEmail);
				          	  	    JSONObject emailSendStatusResponse = gigyaService.setSendEmailInfo(input, dataStored, uId);
				          	  	if(MUConstants.IS_DEBUG_ENABLED) {
				          	  		LOGGER.debug(" Email information in gigya: " + emailSendStatusResponse);
				          	  	}
									
			          	  	}
							
						} else {
							responseObj.put(MUConstants.MAIL_SENT_TO_PARENT, MUConstants.FALSE);
							LOGGER.error("TrackParentConsent : Verification Urls Creation Error.");
							throw new UnexpectedErrorRuntimeException("Verification Urls haven't been created successfully.");
						}	
					} else {
						responseObj.put(MUConstants.MAIL_SENT_TO_PARENT, MUConstants.FALSE);
						LOGGER.error("TrackParentConsent : ParentDetails haven't been set at Gigya.");
						throw new BadRequestRuntimeException("ParentDetails haven't been set at Gigya.");
					}				
				} else {
					responseObj.put(MUConstants.MAIL_SENT_TO_PARENT, MUConstants.FALSE);
					LOGGER.error("TrackParentConsent : Parent details are not present in request in requestbody. "+ requestBodyObj);
					throw new BadRequestRuntimeException("Parent/Uid/U13 details are not present in request.");
				}
			} else {
				responseObj.put(MUConstants.MAIL_SENT_TO_PARENT, MUConstants.FALSE);
				LOGGER.error("TrackParentConsent : RequestBody is not available in request.");
				throw new BadRequestRuntimeException("RequestBody is not available in request.");
			}	
		} catch (BaseServiceRuntimeException e) {
			LOGGER.error("BaseServiceRuntimeException : ", e);
			throw e;
		} catch (Exception e) {
			LOGGER.error("RequestBody is not available in request.", e);
			throw new UnexpectedErrorRuntimeException("Exception in process.");
		}
		return responseObj;
	}
	
	/**
	* @param input instance of JSONObject
	* @param uId instance of String
	* @param locale instance of String
	* @return output instance of String
	*/
	@SuppressWarnings({ "deprecation" })
	private List<String> generateUrl(final JSONObject input, final String uId, final String locale) {
	  
		if(MUConstants.IS_DEBUG_ENABLED) {
			LOGGER.debug("Inside generateUrl with input : " + input.toString());
		}
	  List<String> urlForRegistration = new ArrayList<String>();
	  
	  JSONObject envUrlMap = (JSONObject) input.get(MUConstants.ENVIRONMENT_URL);
	  String envUrl = envUrlMap.get(MUConstants.ENV_URL).toString();
	  long timestamp = System.currentTimeMillis();
	  if(MUConstants.IS_DEBUG_ENABLED) {
		  LOGGER.debug("envUrl : " + envUrl);
		  LOGGER.debug("TimeStamp : " + timestamp);
	  }
	  String acceptedUrl = null;
	  String declinedUrl = null;
	  
	  String key = encryptionKey;
      String initVector = MUConstants.INIT_VECTOR;
	  
      String sTimeStamp = Long.toString(timestamp);
      
      URI acceptUrl = URI.create(MUConstants.UID_PARAM + uId 
	  			+ MUConstants.TIMESTAMP_PARAM + sTimeStamp 
	  			+ MUConstants.CONSENT_PARAM + MUConstants.YES + MUConstants.LOCALE_PARAM + locale);
      
      URI declineUrl = URI.create(MUConstants.UID_PARAM + uId 
	  			+ MUConstants.TIMESTAMP_PARAM + sTimeStamp 
	  			+ MUConstants.CONSENT_PARAM + MUConstants.NO_VALUE + MUConstants.LOCALE_PARAM + locale);
      
      if(MUConstants.IS_DEBUG_ENABLED) {
    	  LOGGER.debug("acceptUrl : " + acceptUrl.toString());
    	  LOGGER.debug("declineUrl : " + declineUrl.toString());
      }
	  
	  String sEncryptedAcceptedUrl = encryptDecrypt.encryptString(acceptUrl.toString(), initVector, key);
	  String sEncryptedDeclinedUrl = encryptDecrypt.encryptString(declineUrl.toString(), initVector, key);
	  
	  if(MUConstants.IS_DEBUG_ENABLED) {
		  LOGGER.debug("sEncryptedAcceptedUrl : " + sEncryptedAcceptedUrl);
		  LOGGER.debug("sEncryptedDeclinedUrl : " + sEncryptedDeclinedUrl);
	  }
	  
	  String sEncodedAcceptUrl = URLEncoder.encode(sEncryptedAcceptedUrl);
	  String sEncodedDeclineUrl = URLEncoder.encode(sEncryptedDeclinedUrl);	 
	  
	  acceptedUrl = envUrl + "/"+ locale + "/parentconsent?pgc=" + sEncodedAcceptUrl;
	  declinedUrl = envUrl + "/"+ locale + "/parentconsent?pgc=" + sEncodedDeclineUrl;
	  if(MUConstants.IS_DEBUG_ENABLED) {
		  LOGGER.debug("acceptedUrl : " + acceptedUrl);
		  LOGGER.debug("declinedUrl : " + declinedUrl);
	  }
	  
	  urlForRegistration.add(acceptedUrl);
	  urlForRegistration.add(declinedUrl);
	  
	  return urlForRegistration;	
	}
	
	/**
	* @param awsStageInput instance of JSONObject
	* @param uId instance of String
	* @param parentFirstName instance of String
	* @param parentLastName instance of String
	* @param parentEmailId instance of String
	* @param relation instance of String
	* @return output instance of boolean
	* @throws ParseException 
	* @throws JsonProcessingException 
	*/
	@SuppressWarnings("unchecked")
	private boolean setParentDetails(final JSONObject awsStageInput, final String uId, final String parentFirstName,
											final String parentLastName, final String parentEmailId,
											final String relation) throws ParseException, JsonProcessingException {
	  
	  LOGGER.debug("Inside setParentDetails.");
	  
	  boolean parentDetailsInsertion = false;
	  try {
		  if ((uId != null) && (!uId.isEmpty()) && (parentFirstName != null) && (!parentFirstName.isEmpty()) 
				  && (parentLastName != null) && (!parentLastName.isEmpty()) 
				  && (parentEmailId != null) && (!parentEmailId.isEmpty())) {
			  			  
			  Map<String, String> dataObj = new HashMap<String, String>();
			  dataObj.put(MUConstants.PARENT_EMAIL_ID, parentEmailId);
			  dataObj.put(MUConstants.PARENT_FIRST_NAME, parentFirstName);
			  dataObj.put(MUConstants.PARENT_LAST_NAME, parentLastName);
			  dataObj.put(MUConstants.PARENT_RELATION, relation);
			  String data = new ObjectMapper().writeValueAsString(dataObj);
			  JSONObject gsRequest = new JSONObject();
			  gsRequest.put(MUConstants.GIGYA_PARAM_UID, uId);
			  gsRequest.put(MUConstants.GIGYA_PARAM_DATA, data);
			  JSONObject gsResponse = new JSONObject();
			  LOGGER.info("TrackParentConsent accountGetInformtaion : data : " + data + " ,uId : "+ uId);
			  gsResponse = getGigyaService().accountSetInformtaion(awsStageInput, gsRequest);
			  LOGGER.info("TrackParentConsent : GigyaService.accountSetInformtaion Success.");
			  if(MUConstants.IS_DEBUG_ENABLED) {
				  LOGGER.debug("Gigya response for accountInfo" + gsResponse);
			  }
			  
			  if ((gsResponse.containsKey(MUConstants.ERROR_CODE)
								&& Integer.parseInt(gsResponse.get(MUConstants.ERROR_CODE).toString()) == 0)) {
					
				  parentDetailsInsertion = true;
				  if(MUConstants.IS_DEBUG_ENABLED) {
					  LOGGER.debug("setParentDetails :" + parentDetailsInsertion);
				  }
					
			  } else {
					LOGGER.error("Response for setParentDetails is unsuccessful at Gigya");
					throw new DependentServiceRuntimeException("DependentServiceError");
			  }
		  } else {
			  LOGGER.info("TrackParentConsent : Input request doesn't contains required details for setParentDetails.");
		  }
	  } catch (ParseException e) {
		  LOGGER.error("TrackParentConsent : ParseException", e);
		  throw new UnexpectedErrorRuntimeException("ParseException");
	  } catch (Exception ex) {
		  LOGGER.error("TrackParentConsent : Error occured while setParentDetails for input : "+  awsStageInput + " ,ExceptionMessage : " + ex.getMessage() + " Exception : " + ex);
			throw new BaseServiceRuntimeException(ex);
	  }
	  	  
	  return parentDetailsInsertion;	
	}


	/**
	 * @return the gigyaService
	 */
	public GigyaService getGigyaService() {
		return gigyaService;
	}

	/**
	 * @param gigyaService the gigyaService to set
	 */
	public void setGigyaService(final GigyaService gigyaService) {
		this.gigyaService = gigyaService;
	}

	/**
	 * @return the exactTargetService
	 */
	public ExactTargetService getExactTargetService() {
		return exactTargetService;
	}

	/**
	 * @param exactTargetService the exactTargetService to set
	 */
	public void setExactTargetService(final ExactTargetService exactTargetService) {
		this.exactTargetService = exactTargetService;
	}

	/**
	 * @return the encryptionKey
	 */
	public String getEncryptionKey() {
		return encryptionKey;
	}

	/**
	 * @param encryptionKey the encryptionKey to set
	 */
	public void setEncryptionKey(final String encryptionKey) {
		this.encryptionKey = encryptionKey;
	}

	/**
	 * @return the encryptDecrypt
	 */
	public EncryptDecrypt getEncryptDecrypt() {
		return encryptDecrypt;
	}

	/**
	 * @param encryptDecrypt the encryptDecrypt to set
	 */
	public void setEncryptDecrypt(EncryptDecrypt encryptDecrypt) {
		this.encryptDecrypt = encryptDecrypt;
	}
	
}
