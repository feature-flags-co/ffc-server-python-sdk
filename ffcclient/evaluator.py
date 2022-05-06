import base64
import json
import re
from typing import Callable, Optional, Tuple

from ffcclient.common_types import EvalDetail, FFCEvent, FFCUser
from ffcclient.event_types import FlagEventVariation
from ffcclient.utils import is_numeric, log, unpack_feature_flag_id
from ffcclient.utils.variation_splitting_algorithm import \
    VariationSplittingAlgorithm

REASON_CLIENT_NOT_READY = 'client not ready'

REASON_FLAG_NOT_FOUND = 'flag not found'

REASON_ERROR = 'error in evaluation'

REASON_USER_NOT_SPECIFIED = 'user not specified'

REASON_WRONG_TYPE = 'wrong type'

__FLAG_DISABLE_STATS__ = 'Disabled'

__REASON_FLAG_OFF__ = 'flag off'

__REASON_PREREQUISITE_FAILED__ = 'prerequisite failed'

__REASON_TARGET_MATCH__ = 'target match'

__REASON_RULE_MATCH__ = 'rule match'

__REASON_FALLTHROUGH__ = 'fall through all rules'


__THAN_CLAUSE__ = 'Than'

__GE_CLAUSE__ = 'BiggerEqualThan'

__GT_CLAUSE__ = 'BiggerThan'

__LE_CLAUSE__ = 'LessEqualThan'

__LT_CLAUSE__ = 'LessThan'

__EQ_CLAUSE__ = 'Equal'

__NEQ_CLAUSE__ = 'NotEqual'

__CONTAINS_CLAUSE__ = 'Contains'

__NOT_CONTAIN_CLAUSE__ = 'NotContain'

__IS_ONE_OF_CLAUSE__ = 'IsOneOf'

__NOT_ONE_OF_CLAUSE__ = 'NotOneOf'

__STARTS_WITH_CLAUSE__ = 'StartsWith'

__ENDS_WITH_CLAUSE__ = 'EndsWith'

__IS_TRUE_CLAUSE__ = 'IsTrue'

__IS_FALSE_CLAUSE__ = 'IsFalse'

__MATCH_REGEX_CLAUSE__ = 'MatchRegex'

__NOT_MATCH_REGEX_CLAUSE__ = 'NotMatchRegex'

__IS_IN_SEGMENT_CLAUSE__ = 'User is in segment'

__NOT_IN_SEGMENT_CLAUSE__ = 'User is not in segment'


class Evaluator:
    def __init__(self,
                 flag_getter: Callable[[str], dict],
                 segment_getter: Callable[[str], dict]):
        self.__flag_getter = flag_getter
        self.__segment_getter = segment_getter
        self.__ops = {'disabled_user_variation': self._match_feature_flag_disabled_user_variation,
                      'targeted_user_variation': self._match_targeted_user_variation,
                      'condition_user_variation': self._match_condition_user_variation,
                      'default_user_variation': self._match_default_user_variation}

    def evaluate(self, flag: dict, user: FFCUser, ffc_event: Optional[FFCEvent] = None) -> EvalDetail:
        if not flag or not user:
            raise ValueError('null flag or empty user')
        return self._match_user_variation(flag, user, ffc_event)

    def _match_user_variation(self, flag: dict, user: FFCUser, ffc_event: FFCEvent) -> EvalDetail:
        try:
            is_send_to_expt = False
            ed = None
            for name, op in self.__ops.items():
                if name == 'disabled_user_variation':
                    is_send_to_expt, ed = op(flag, user, ffc_event)
                else:
                    is_send_to_expt, ed = op(flag, user)
                if ed:
                    return ed
        finally:
            if ed:
                log.info('FFC Python SDK: User %s, Feature Flag %s, Flag Value %s' % (user.get('KeyId'), ed.key_name, ed.variation))
                if ffc_event:
                    ffc_event.add(FlagEventVariation(flag['ff']['keyName'], is_send_to_expt, ed))

    #  return a value when flag is off or not match prerequisite rule
    def _match_feature_flag_disabled_user_variation(self, flag: dict, user: FFCUser, ffc_event: FFCEvent) -> Tuple[bool, Optional[EvalDetail]]:
        if flag['ff']['status'] == __FLAG_DISABLE_STATS__:
            return False, EvalDetail(flag['ff']['variationOptionWhenDisabled']['localId'],
                                     __REASON_FLAG_OFF__,
                                     flag['ff']['variationOptionWhenDisabled']['variationValue'],
                                     flag['ff']['keyName'],
                                     flag['ff']['name'])

        is_match_prerequisite = True
        for prerequisite in flag['ffp']:
            if prerequisite['prerequisiteFeatureFlagId'] != flag['ff']['id']:
                pre_flag = self.__flag_getter(prerequisite['prerequisiteFeatureFlagId'])
                if not pre_flag:
                    pre_flag_key = unpack_feature_flag_id(prerequisite['prerequisiteFeatureFlagId'], 4)
                    log.warn('FFC Python SDK: prerequisite flag %s not found' % pre_flag_key)
                    is_match_prerequisite = False
                    break

                ed = self._match_user_variation(pre_flag, user, ffc_event)
                if ed.id != prerequisite['ValueOptionsVariationValue']['localId']:
                    is_match_prerequisite = False
                    break

        if not is_match_prerequisite:
            return False, EvalDetail(flag['ff']['variationOptionWhenDisabled']['localId'],
                                     __REASON_PREREQUISITE_FAILED__,
                                     flag['ff']['variationOptionWhenDisabled']['variationValue'],
                                     flag['ff']['keyName'],
                                     flag['ff']['name'])
        return False, None

    # return the value of target user
    def _match_targeted_user_variation(self, flag: dict, user: FFCUser) -> Tuple[bool, Optional[EvalDetail]]:
        for target in flag['targetIndividuals']:
            if any(individual['keyId'] == user.get('KeyId') for individual in target['individuals']):
                return self._is_send_to_expt_for_targeted_user_variation(flag['exptIncludeAllRules']), EvalDetail(target['valueOption']['localId'],
                                                                                                                  __REASON_TARGET_MATCH__,
                                                                                                                  target['valueOption']['variationValue'],
                                                                                                                  flag['ff']['keyName'],
                                                                                                                  flag['ff']['name'])
        return False, None

    # return the value of matched rule
    def _match_condition_user_variation(self, flag: dict, user: FFCUser) -> Tuple[bool, Optional[EvalDetail]]:
        for rule in flag['fftuwmtr']:
            if self._match_any_rule(user, rule):
                return self._get_rollout_variation_option(rule['valueOptionsVariationRuleValues'],
                                                          user,
                                                          __REASON_RULE_MATCH__,
                                                          flag['exptIncludeAllRules'],
                                                          rule['isIncludedInExpt'],
                                                          flag['ff']['keyName'],
                                                          flag['ff']['name'])
        return False, None

    # get value from default rule
    def _match_default_user_variation(self, flag: dict, user: FFCUser) -> Tuple[bool, Optional[EvalDetail]]:
        return self._get_rollout_variation_option(flag['ff']['defaultRulePercentageRollouts'],
                                                  user,
                                                  __REASON_FALLTHROUGH__,
                                                  flag['exptIncludeAllRules'],
                                                  flag['ff']['isDefaultRulePercentageRolloutsIncludedInExpt'],
                                                  flag['ff']['keyName'],
                                                  flag['ff']['name'])

    def _match_any_rule(self, user: FFCUser, rule: dict) -> bool:
        all_match = False
        for clause in rule['ruleJsonContent']:
            if not self._process_clause(user, clause):
                all_match = False
                break
            all_match = True
        return all_match

    def _process_clause(self, user: FFCUser, clause: dict) -> bool:
        op = clause['operation']
        # segment hasn't any operation
        op = clause['property'] if not op else op
        if __THAN_CLAUSE__ in str(op):
            return self._than(user, clause)
        elif op == __EQ_CLAUSE__:
            return self._equals(user, clause)
        elif op == __NEQ_CLAUSE__:
            return not self._equals(user, clause)
        elif op == __CONTAINS_CLAUSE__:
            return self._contains(user, clause)
        elif op == __NOT_CONTAIN_CLAUSE__:
            return not self._contains(user, clause)
        elif op == __IS_ONE_OF_CLAUSE__:
            return self._one_of(user, clause)
        elif op == __NOT_ONE_OF_CLAUSE__:
            return not self._one_of(user, clause)
        elif op == __STARTS_WITH_CLAUSE__:
            return self._starts_with(user, clause)
        elif op == __ENDS_WITH_CLAUSE__:
            return self._ends_with(user, clause)
        elif op == __IS_TRUE_CLAUSE__:
            return self._true(user, clause)
        elif op == __IS_FALSE_CLAUSE__:
            return self._false(user, clause)
        elif op == __MATCH_REGEX_CLAUSE__:
            return self._match_reg_exp(user, clause)
        elif op == __NOT_MATCH_REGEX_CLAUSE__:
            return not self._match_reg_exp(user, clause)
        elif op == __IS_IN_SEGMENT_CLAUSE__:
            return self._in_segment(user, clause)
        elif op == __NOT_IN_SEGMENT_CLAUSE__:
            return not self._in_segment(user, clause)
        else:
            return False

    def _than(self, user: FFCUser, clause: dict) -> bool:
        pv = user.get(clause['property'])
        if not is_numeric(pv) or not is_numeric(clause['value']):
            return False
        pv_num, cv_num = round(float(pv), 5), round(float(clause['value']), 5)
        op = clause['operation']
        if op == __GE_CLAUSE__:
            return pv_num >= cv_num
        elif op == __GT_CLAUSE__:
            return pv_num > cv_num
        elif op == __LE_CLAUSE__:
            return pv_num <= cv_num
        elif op == __LT_CLAUSE__:
            return pv_num < cv_num
        else:
            return False

    def _equals(self, user: FFCUser, clause: dict) -> bool:
        pv = user.get(clause['property'])
        cv = clause['value']
        return pv and cv and str(pv) == str(cv)

    def _contains(self, user: FFCUser, clause: dict) -> bool:
        pv = user.get(clause['property'])
        cv = clause['value']
        return pv and cv and str(cv) in str(pv)

    def _one_of(self, user: FFCUser, clause: dict) -> bool:
        pv = user.get(clause['property'])
        try:
            cv = json.loads(clause['value'])
            return pv and cv and str(pv) in cv
        except:
            return False

    def _starts_with(self, user: FFCUser, clause: dict) -> bool:
        pv = user.get(clause['property'])
        cv = clause['value']
        return pv and cv and str(pv).startswith(str(cv))

    def _ends_with(self, user: FFCUser, clause: dict) -> bool:
        pv = user.get(clause['property'])
        cv = clause['value']
        return pv and cv and str(pv).endswith(str(cv))

    def _true(self, user: FFCUser, clause: dict) -> bool:
        pv = user.get(clause['property'])
        return pv and str(pv).lower() == 'true'

    def _false(self, user: FFCUser, clause: dict) -> bool:
        pv = user.get(clause['property'])
        return pv and str(pv).lower() == 'false'

    def _match_reg_exp(self, user: FFCUser, clause: dict) -> bool:
        pv = user.get(clause['property'])
        cv = clause['value']
        return pv and cv and re.search(str(cv), str(pv), flags=re.I)

    def _in_segment(self, user: FFCUser, clause: dict) -> bool:
        def is_match_user(user: FFCUser, segment: dict) -> bool:
            if not user or not segment:
                return False
            user_key = user.get('KeyId')
            if user_key in segment['excluded']:
                return False
            if user_key in segment['included']:
                return True
            return any(self._match_any_rule(user, rule) for rule in segment.get('rules', []))
        try:
            cv = json.loads(clause['value'])
            return cv and any(is_match_user(user, self.__segment_getter(sgid)) for sgid in cv)
        except:
            return False

    def _get_rollout_variation_option(self,
                                      rollouts: dict,
                                      user: FFCUser,
                                      reason: str,
                                      expt_include_all_rules: bool,
                                      rule_inclued_in_expt: bool,
                                      key_name: str,
                                      name: str) -> Tuple[bool, Optional[EvalDetail]]:

        user_key = user.get('KeyId')
        for rollout in rollouts:
            if VariationSplittingAlgorithm(user_key, rollout['rolloutPercentage']).is_key_belongs_to_percentage():
                send_to_expt = self._is_send_to_expt(user_key, rollout, expt_include_all_rules, rule_inclued_in_expt)
                return send_to_expt, EvalDetail(rollout['valueOption']['localId'],
                                                reason,
                                                rollout['valueOption']['variationValue'],
                                                key_name,
                                                name)
        return False, None

    def _is_send_to_expt_for_targeted_user_variation(self, expt_include_all_rules: bool) -> bool:
        return expt_include_all_rules is None or expt_include_all_rules

    def _is_send_to_expt(self,
                         user_key: str,
                         rollout: dict,
                         expt_include_all_rules: bool,
                         rule_inclued_in_expt: bool) -> bool:
        if expt_include_all_rules is None:
            return True
        if rule_inclued_in_expt is None or rollout['exptRollout'] is None:
            return True
        if not rule_inclued_in_expt:
            return False

        send_to_expt_percentage = rollout['exptRollout']
        splitting_percentage = rollout['rolloutPercentage'][1] - rollout['rolloutPercentage'][0]

        if send_to_expt_percentage == 0 or splitting_percentage == 0:
            return False

        upper_bound = send_to_expt_percentage / splitting_percentage
        if upper_bound > 1:
            upper_bound = 1
        new_user_key = base64.b64encode(user_key.encode()).decode()
        return VariationSplittingAlgorithm(new_user_key, [0, upper_bound]).is_key_belongs_to_percentage()
