package study.querydslstudy;

import com.querydsl.core.QueryResults;
import com.querydsl.core.Tuple;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.dsl.CaseBuilder;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.jpa.JPAExpressions;
import com.querydsl.jpa.impl.JPAQueryFactory;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.PersistenceUnit;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;
import study.querydslstudy.entity.Member;
import study.querydslstudy.entity.QMember;
import study.querydslstudy.entity.QTeam;
import study.querydslstudy.entity.Team;

import java.util.List;

import static org.assertj.core.api.Assertions.*;
import static study.querydslstudy.entity.QMember.*;
import static study.querydslstudy.entity.QTeam.*;

@SpringBootTest
@Transactional
public class QuerydslBasicTest {

    @Autowired
    EntityManager em;

    JPAQueryFactory qf;

    @BeforeEach
    public void before(){
        qf = new JPAQueryFactory(em);

        Team teamA = new Team("teamA");
        Team teamB = new Team("teamB");
        em.persist(teamA);
        em.persist(teamB);

        Member m1 = new Member("m1", 15, teamA);
        Member m2 = new Member("m2", 21, teamA);

        Member m3 = new Member("m3", 52, teamB);
        Member m4 = new Member("m4", 62, teamB);

        em.persist(m1);
        em.persist(m2);
        em.persist(m3);
        em.persist(m4);
    }

    /**
     * JPQL과 Querydsl의 차이
     */
    @Test
    public void startJPQL(){
        //m1을 찾아라
        Member findMember = em.createQuery("select m from Member m where m.username = :username", Member.class)
                .setParameter("username", "m1")
                .getSingleResult();

        assertThat(findMember.getUsername()).isEqualTo("m1");

        //JPQL 쿼리를 잘못입력했을때 런타임때 오류를 확인한다.
    }

    @Test
    public void startQuerydsl(){
//        JPAQueryFactory qf = new JPAQueryFactory(em); // (이걸 필드로 뺄수 있다.)

//        QMember m = new QMember("m"); //new QMember(어떤 q 멤버인지 구분하는 것) 
//        Q클래스 인스턴스를 사용하는 방법 1 (같은 테이블을 선언해야 하는 경우에 주로 사용)

          QMember m = member;//Q클래스 인스턴스를 사용하는 방법 2

        Member findMember = qf
                        .select(member) //Q클래스를 인스턴스를 사용하는 방법 QMember.member -> 스태틱 인스턴스 사용해서 줄여서 사용
                        .from(member)
                        .where(member.username.eq("m1")) // 파람 바인딩 처리
                        .fetchOne();

        assertThat(findMember.getUsername()).isEqualTo("m1");

        //Querydsl 쿼리를 잘못입력했을때 컴파일때 오류를 확인 할수 있다.
    }

    /**
     * 검색조건 Querydsl 문법
     */
    @Test 
    public void search(){
        Member findMember = qf
                .selectFrom(member) // select from이 같으면 합쳐도 된다.
                .where(member.username.eq("m1")
                        .and(member.age.between(14, 60)))
                .fetchOne();

        assertThat(findMember.getUsername()).isEqualTo("m1");
    }

    @Test
    public void searchAndParam(){
        Member findMember = qf
                .selectFrom(member) // select from이 같으면 합쳐도 된다.
                .where(member.username.eq("m1"),
//                        ,로 and를 생략할수도 있다, 중간에 null 이 들어가면 null 을 무시한다 (동적쿼리 짤때 유용)
                        (member.age.eq(15)))
                .fetchOne();

        assertThat(findMember.getUsername()).isEqualTo("m1");
    }

    /**
     * 결과 조회
     */

    @Test
    public void resultFetch(){

//        List<Member> fetch = qf
//                .selectFrom(member)
//                .fetch(); //List
//
//        Member fetchOne = qf
//                .selectFrom(member)
//                .fetchOne(); //한건 조회
//
//        Member fetchFirst = qf
//                .selectFrom(member)
//                .fetchFirst(); //처음 한건 조회

        QueryResults<Member> result = qf
                .selectFrom(member)
                .fetchResults(); //페이징 시 사용

        result.getTotal();
        List<Member> content = result.getResults(); // 페이징시 사용

//        long total = qf
//                .selectFrom(member)
//                .fetchCount(); // 카운트 쿼리만 가져온다.

    }

    /**
     * 회원 정렬 순서
     * 1.회원 나이 내림차순(desc)
     * 2. 회원 이름 올림차순(asc)
     * 단 2에서 회원 이름이 없으면 마지막에 출력 (nulls last)
     */
    @Test
    public void sort(){
        em.persist(new Member(null, 100));
        em.persist(new Member("m7", 100));
        em.persist(new Member("m8", 100));

        List<Member> result = qf
                .selectFrom(member)
                .where(member.age.eq(100))
                .orderBy(member.age.desc(), member.username.asc().nullsLast())
                .fetch();

        Member m7 = result.get(0);
        Member m8 = result.get(1);
        Member membernull = result.get(2);
        assertThat(m7.getUsername()).isEqualTo("m7");
        assertThat(m8.getUsername()).isEqualTo("m8");
        assertThat(membernull.getUsername()).isNull();
    }

    /**
     * 페이징 처리
     */

    @Test
    public void paging1(){
        List<Member> result = qf
                .selectFrom(member)
                .orderBy(member.username.desc())
                .offset(1)
                .limit(2)
                .fetch();

        assertThat(result.size()).isEqualTo(2);
    }

    //
    @Test
    public void paging2(){
        QueryResults<Member> queryResults = qf
                .selectFrom(member)
                .orderBy(member.username.desc())
                .offset(1)
                .limit(2)
                .fetchResults();

        assertThat(queryResults.getTotal()).isEqualTo(4);
        assertThat(queryResults.getLimit()).isEqualTo(2);
        assertThat(queryResults.getOffset()).isEqualTo(1);
        assertThat(queryResults.getResults()).size().isEqualTo(2);
    }

    /**
     * 집합
     */

    @Test
    public void aggregation(){
        //Tuple = Querydsl에서 제공하는 Tuple (여러개의 타입에서 꺼낼수 있는것)
        List<Tuple> result = qf
                .select(
                        member.count(),
                        member.age.sum(),
                        member.age.avg(),
                        member.age.max(),
                        member.age.min()
                )
                .from(member)
                .fetch();

        Tuple tuple = result.get(0);
        assertThat(tuple.get(member.count())).isEqualTo(4);
        assertThat(tuple.get(member.age.sum())).isEqualTo(150);
        assertThat(tuple.get(member.age.avg())).isEqualTo(37.5);
        assertThat(tuple.get(member.age.max())).isEqualTo(62);
        assertThat(tuple.get(member.age.min())).isEqualTo(15);
    }

    /**
     * 팀 명과 각 팀의 평균 연령을 구하라.
     */
    @Test
    public void grouping() throws Exception {
        List<Tuple> result = qf
                .select(team.name, member.age.avg())
                .from(member)
                .join(member.team, team)
                .groupBy(team.name)
                .fetch();

        Tuple teamA = result.get(0);
        Tuple teamB = result.get(1);

        assertThat(teamA.get(team.name)).isEqualTo("teamA");
        assertThat(teamA.get(member.age.avg())).isEqualTo(18);


        assertThat(teamB.get(team.name)).isEqualTo("teamB");
        assertThat(teamB.get(member.age.avg())).isEqualTo(57);

        //sql처럼 having도 넣을 수 있다.
    }

    /**
     * 팀 A에 소속된 모든 회원
     */
    @Test
    public void join(){
        List<Member> result = qf
                .selectFrom(member)
                .join(member.team, team) //left right 도 가능
                .where(team.name.eq("teamA"))
                .fetch();

        assertThat(result)
                .extracting("username")
                .containsExactly("m1", "m2");
    }

    /**
     * 세타 조인 - 회원의 이름이 팀 이름과 같은 회원 조회
     */
    @Test
    public void thetaJoin(){
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));
        List<Member> result = qf
                .select(member)
                .from(member, team)
                .where(team.name.eq("teamA"))
                .fetch();

        assertThat(result)
                .extracting("username")
                .containsExactly("teamA", "teamB");
        // 모든 Member 테이블, Team 테이블 다 조인하고 (from 절에 여러 엔티티를 선택해 세타 조인)
        // Member, Team 이름이 같은 결과를 조사한다.
        // 외부 조인 불가능, 조인 on 사용하면 외부 조인 가능
    }

    /**
     * 조인 - ON절을 활용한 조인 1. 조인 대상 필터링
     * 회원과 팀을 조인하면서 팀 이름이 teamA 만 조인, 회원은 모드 조회
     * JPQL : select m , from Member m left join m.team t on t.name = 'teamA'
     */
    @Test
    public void joinOnFiltering(){
        List<Tuple> result = qf
                .select(member, team)
                .from(member)
                .join(member.team, team)
                .where(team.name.eq("teamA"))
//                .leftJoin(member.team, team)
//                .on(team.name.eq("teamA"))
                .fetch();

        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }
        // inner 조인과 기능이 비슷하기 때문에 이런 케이스는 익숙한 where 절을 사용하고
        // 외부조인이 필요한 경우에만 on절을 사용하자.

    }

    /**
     * 연관관계가 없는 엔티티 외부 조인
     * (회원의 이름이 팀 이름과 같은 대상 외부 조인)
     */
    @Test
    public void joinOnNooRelation(){
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));
        em.persist(new Member("teamC"));

        List<Tuple> result = qf
                .select(member, team)
                .from(member)
                .leftJoin(team).on(member.username.eq(team.name))
//                .leftJoin(member.team, team) 보통 이렇게 하는데
//                member.team,를 뺴면 join 대상으로 이름만 필터링 된다.
                .fetch();

        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }
        // inner 조인과 기능이 비슷하기 때문에 이런 케이스는 익숙한 where 절을 사용하고
        // 외부조인이 필요한 경우에만 on절을 사용하자.
    }

    /**
     * 페치조인
     */

    @PersistenceUnit
    EntityManagerFactory enf;
    @Test
    public void fetchJoinNo(){
        em.flush();
        em.clear();

        Member findMember = qf
                .selectFrom(member)
                .where(member.username.eq("m1"))
                .fetchOne();

        boolean loaded = enf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());
        assertThat(loaded).as("Fetch Join is not allow").isFalse();
    }

    @Test
    public void fetchJoin(){
        em.flush();
        em.clear();

        Member findMember = qf
                .selectFrom(member)
                .join(member.team, team).fetchJoin()
                .where(member.username.eq("m1"))
                .fetchOne();

        boolean loaded = enf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());
        assertThat(loaded).as("Fetch Join is not allow").isTrue();
    }

    /**
     * 서브쿼리 - 나이가 가장 많은 회원 조회
     */
    @Test
    public void subQuery(){

        QMember memberSub = new QMember("memberSub");

        List<Member> result = qf
                .selectFrom(member)
                .where(member.age.eq(
                        JPAExpressions
                                .select(memberSub.age.max())
                                .from(memberSub)
                ))
                .fetch();

        assertThat(result)
                .extracting("age")
                .containsExactly(62);
    }   
    
    /**
     * 서브쿼리 - 나이가 평균이상인 회원 조회
     */
    @Test
    public void subQueryGoe(){

        QMember memberSub = new QMember("memberSub");

        List<Member> result = qf
                .selectFrom(member)
                .where(member.age.goe(
                        JPAExpressions
                                .select(memberSub.age.avg())
                                .from(memberSub)
                ))
                .fetch();

        assertThat(result)
                .extracting("age")
                .containsExactly(52,62);
    }

    /**
     * 서브쿼리 - 나이가 초과인 경우
     */
    @Test
    public void subQueryIn(){

        QMember memberSub = new QMember("memberSub");

        List<Member> result = qf
                .selectFrom(member)
                .where(member.age.in(
                        JPAExpressions
                                .select(memberSub.age)
                                .from(memberSub)
                                .where(memberSub.age.gt(20))
                ))
                .fetch();

        assertThat(result)
                .extracting("age")
                .containsExactly(21,52,62);
    } 
    
    /**
     * 서브쿼리 - 셀렉트 서브쿼리
     */
    @Test
    public void selectSubQuery(){

        QMember memberSub = new QMember("memberSub");

        List<Tuple> result = qf
                .select(member.username,
                        JPAExpressions
                                .select(memberSub.age.avg())
                                .from(memberSub))
                        .from(member)
                        .fetch();

        for (Tuple Tuple : result) {
            System.out.println("Tuple = " + Tuple);
        }
        //JPA는 from 절의 서브쿼리가 불가능 하다.
        //=> 해결 방안 : 1. 서브 쿼리를 join으로 변경
        // , 2. 애플리케이션에서 쿼리를 2번 분리해서 사용
        // , 3. nativeSQL 사용
    }

    /**
     * Case문
     *
     */
    
    @Test
    public void baseCase() throws Exception {
        List<String> result = qf
                .select(member.age
                        .when(10).then("열살")
                        .when(20).then("스무살")
                        .otherwise("틀딱"))
                .from(member)
                .fetch();

        for (String s : result) {
            System.out.println("s = " + s);
        }

    }

    @Test
    public void complexCase() throws Exception {
        List<String> result = qf
                .select(new CaseBuilder()
                                .when(member.age.between(0,20)).then("0~20살")
                                .when(member.age.between(21,30)).then("21~30살")
                                .otherwise("틀딱")
                        )
                .from(member)
                .fetch();

        for (String s : result) {
            System.out.println("s = " + s);
        }

    }

    /**
     * 상수
     */
    @Test
    public void constant(){
        List<Tuple> result = qf
                .select(member.username, Expressions.constant("A"))
                .from(member)
                .fetch();

        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }
    }

    @Test
    public void concat(){
        List<String> result = qf
                .select(member.username.concat("_").concat(member.age.stringValue()))
                .from(member)
                .where(member.username.eq("m1"))
                .fetch();

        for (String o : result) {
            System.out.println("o = " + o);

        }
    }

}

