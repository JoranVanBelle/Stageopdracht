package com.stage.adapter.mvb.repository;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import com.stage.adapter.mvb.entity.Emailaddress;

@ExtendWith(MockitoExtension.class)
public class EmailRepositoryTest {

	private EmailRepository emailRepository;
	
	private NamedParameterJdbcTemplate jdbcTemplate = Mockito.mock(NamedParameterJdbcTemplate.class);
	
	@BeforeEach
	public void setup() {
		emailRepository = new EmailRepository(jdbcTemplate);
	}
	
	@Test
	public void collectEmailAddressesTest() {
		List<Map<String, Object>> mockResult = new ArrayList<>();
        Map<String, Object> map1 = new HashMap<>();
        map1.put("email", "test@email.com");
        
        Map<String, Object> map2 = new HashMap<>();
        map2.put("email", "test2@email.com");
        
        mockResult.add(map2);
        mockResult.add(map1);
		
		Mockito.when(jdbcTemplate.queryForList(Mockito.anyString(), Mockito.any(SqlParameterSource.class)))
		.thenReturn(mockResult);
		
		List<Emailaddress> result = emailRepository.collectEmailAddresses("Nieuwpoort");
		
		Assertions.assertEquals(2, result.size());
		Assertions.assertEquals("test2@email.com", result.get(0).getEmailaddress());
		Assertions.assertEquals("test@email.com", result.get(1).getEmailaddress());
	}
	
}
